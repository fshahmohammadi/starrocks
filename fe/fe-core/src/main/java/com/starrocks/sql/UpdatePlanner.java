// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SchemaTableSink;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UpdatePlanner {

    public ExecPlan plan(UpdateStmt updateStmt, ConnectContext session) {
        QueryRelation query = updateStmt.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        Table targetTable = updateStmt.getTable();

        //1. Cast output columns type to target type
        OptExprBuilder optExprBuilder = logicalPlan.getRootBuilder();
        optExprBuilder = castOutputColumnsTypeToTargetColumns(columnRefFactory, targetTable,
                colNames, outputColumns, optExprBuilder);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(updateStmt.getTable());
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            long tableId = targetTable.getId();
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setUpdateTableId(tableId);

            // Dict passthrough: compute passthrough candidates (non-key assigned columns)
            List<ColumnRefOperator> passthroughColumns = computeUpdatePassthroughColumns(
                    targetTable, updateStmt, outputColumns, colNames);
            optimizerContext.setSinkPassthroughOutputColumns(passthroughColumns);

            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(
                    optExprBuilder.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(outputColumns));

            // Read passthrough result and build effective output columns
            Map<String, Integer> passthroughColumnToDictRefSlotId = new HashMap<>();
            Map<String, Type> passthroughColumnType = new HashMap<>();
            Map<Integer, Integer> passthroughSourceSlotMap = new HashMap<>();
            List<ColumnRefOperator> effectiveOutputColumns = new ArrayList<>(outputColumns);
            Map<Integer, Integer> passthroughResult = optimizerContext.getSinkDictPassthroughResult();
            if (!passthroughResult.isEmpty()) {
                // Build the column schema matching outputColumns order
                // (for partial update: key columns + assigned columns, in full schema order)
                List<Column> updateSchema = buildUpdateSchema(targetTable, updateStmt);
                for (int i = 0; i < outputColumns.size() && i < updateSchema.size(); i++) {
                    Integer dictRefId = passthroughResult.get(outputColumns.get(i).getId());
                    if (dictRefId != null) {
                        ColumnRefOperator dictRefCol = columnRefFactory.getColumnRef(dictRefId);
                        passthroughColumnToDictRefSlotId.put(
                                updateSchema.get(i).getName(), dictRefId);
                        passthroughColumnType.put(
                                updateSchema.get(i).getName(), dictRefCol.getType());
                        effectiveOutputColumns.set(i, dictRefCol);
                    }
                }
            }

            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    effectiveOutputColumns, columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
            for (Column column : targetTable.getFullSchema()) {
                if (updateStmt.usePartialUpdate() && !column.isGeneratedColumn() &&
                        !updateStmt.isAssignmentColumn(column.getName()) && !column.isKey()) {
                    // When using partial update, skip columns which aren't key column and not be assign, except for
                    // generated column
                    continue;
                }
                SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
                Integer dictRefSlotId = passthroughColumnToDictRefSlotId.get(column.getName());
                if (dictRefSlotId != null) {
                    Type dictType = passthroughColumnType.get(column.getName());
                    slotDescriptor.setType(dictType);
                    slotDescriptor.setOriginType(dictType);
                    passthroughSourceSlotMap.put(slotDescriptor.getId().asInt(), dictRefSlotId);
                } else {
                    slotDescriptor.setType(column.getType());
                }
                if (column.getType().isVarchar() &&
                        IDictManager.getInstance().hasGlobalDict(tableId, column.getColumnId())) {
                    Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(tableId, column.getColumnId());
                    dict.ifPresent(
                            columnDict -> globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), columnDict)));
                }
            }
            olapTuple.computeMemLayout();

            if (targetTable instanceof OlapTable) {
                List<Long> partitionIds = Lists.newArrayList();
                for (Partition partition : targetTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
                OlapTable olapTable = (OlapTable) targetTable;
                DataSink dataSink = new OlapTableSink(olapTable, olapTuple, partitionIds, olapTable.writeQuorum(),
                        olapTable.enableReplicatedStorage(), false,
                        olapTable.supportedAutomaticPartition(), session.getCurrentComputeResource());
                if (updateStmt.usePartialUpdate()) {
                    // using column mode partial update in UPDATE stmt
                    ((OlapTableSink) dataSink).setPartialUpdateMode(TPartialUpdateMode.COLUMN_UPDATE_MODE);
                }
                if (session.getTxnId() != 0) {
                    ((OlapTableSink) dataSink).setIsMultiStatementsTxn(true);
                }

                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                sinkFragment.setSink(dataSink);
                sinkFragment.setLoadGlobalDicts(globalDicts);

                // Wire up dict passthrough on sink and fragment
                if (!passthroughSourceSlotMap.isEmpty()) {
                    ((OlapTableSink) dataSink).setDictPassthroughColumnNames(
                            new ArrayList<>(passthroughColumnToDictRefSlotId.keySet()));
                    sinkFragment.setDictPassthroughSourceSlotMap(passthroughSourceSlotMap);
                    PlanFragment lastFragment = execPlan.getFragments().get(
                            execPlan.getFragments().size() - 1);
                    sinkFragment.mergeQueryGlobalDicts(lastFragment.getQueryGlobalDicts());
                    if (lastFragment.getQueryGlobalDictExprs() != null) {
                        sinkFragment.mergeQueryDictExprs(lastFragment.getQueryGlobalDictExprs());
                    }
                }

                // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
                session.getSessionVariable().setPreferComputeNode(false);
                session.getSessionVariable().setUseComputeNodes(0);
                OlapTableSink olapTableSink = (OlapTableSink) dataSink;
                TableRef tableRef = updateStmt.getTableRef();
                TableName catalogDbTable = TableName.fromTableRef(tableRef);
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogDbTable.getCatalog(),
                        catalogDbTable.getDb());
                try {
                    olapTableSink.init(session.getExecutionId(), updateStmt.getTxnId(), db.getId(), session.getExecTimeout());
                    olapTableSink.complete();
                } catch (StarRocksException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else if (targetTable instanceof SystemTable) {
                DataSink dataSink = new SchemaTableSink((SystemTable) targetTable,
                        ConnectContext.get().getCurrentComputeResource());
                execPlan.getFragments().get(0).setSink(dataSink);
            } else {
                throw new SemanticException("Unsupported table type: " + targetTable.getClass().getName());
            }
            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                SessionVariable sv = session.getSessionVariable();
                if (sv.getEnableAdaptiveSinkDop()) {
                    long warehouseId = session.getCurrentComputeResource().getWarehouseId();
                    sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
                } else {
                    sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
                }
                if (targetTable instanceof OlapTable) {
                    sinkFragment.setHasOlapTableSink();
                }
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
                sinkFragment.disableRuntimeAdaptiveDop();
            } else {
                execPlan.getFragments().get(0).setPipelineDop(1);
            }
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    /**
     * Cast output columns type to target type.
     * @param columnRefFactory :  column ref factory of update stmt.
     * @param targetTable: target table of update stmt.
     * @param colNames: column names of update stmt.
     * @param outputColumns: output columns of update stmt.
     * @param root: root logical plan of update stmt.
     * @return: new root logical plan with cast operator.
     */
    private static OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                                       Table targetTable,
                                                                       List<String> colNames,
                                                                       List<ColumnRefOperator> outputColumns,
                                                                       OptExprBuilder root) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rewriteRules = Arrays.asList(new FoldConstantsRule());
        Preconditions.checkState(colNames.size() == outputColumns.size(), "Column name's size %s should be equal " +
                "to output column refs' size %s", colNames.size(), outputColumns.size());

        for (int columnIdx = 0; columnIdx < outputColumns.size(); ++columnIdx) {
            ColumnRefOperator outputColumn = outputColumns.get(columnIdx);
            String colName = colNames.get(columnIdx);
            // It's safe to use getColumn directly, because the column name's case-insensitive is the same with table's schema.
            Column column = targetTable.getColumn(colName);
            Preconditions.checkState(column != null, "Column %s not found in table %s", colName,
                    targetTable.getName());
            if (!column.getType().matchesType(outputColumn.getType())) {
                // This should be always true but add a check here to avoid updating the wrong column type.
                if (!TypeManager.canCastTo(outputColumn.getType(), column.getType())) {
                    throw new SemanticException(String.format("Output column type %s is not compatible table column type: %s",
                            outputColumn.getType(), column.getType()));
                }
                ColumnRefOperator k = columnRefFactory.create(column.getName(), column.getType(), column.isAllowNull());
                ScalarOperator castOperator = new CastOperator(column.getType(), outputColumn, true);
                columnRefMap.put(k, rewriter.rewrite(castOperator, rewriteRules));
                outputColumns.set(columnIdx, k);
            } else {
                columnRefMap.put(outputColumn, outputColumn);
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    /**
     * Compute passthrough candidates for UPDATE: non-key assigned columns.
     * These columns can skip dict decoding and pass INT dict codes directly to the sink.
     */
    private List<ColumnRefOperator> computeUpdatePassthroughColumns(
            Table targetTable, UpdateStmt updateStmt,
            List<ColumnRefOperator> outputColumns, List<String> colNames) {
        if (!(targetTable instanceof OlapTable)) {
            return List.of();
        }
        // Dict passthrough is not supported in shared-data (lake/cloud-native) mode
        // because the lake DeltaWriter does not support passthrough_source_dicts.
        if (targetTable.isCloudNativeTableOrMaterializedView()) {
            return List.of();
        }
        Set<String> keyColumnNames = InsertPlanner.getKeyColumnNames((OlapTable) targetTable);

        List<ColumnRefOperator> result = new ArrayList<>();
        for (int i = 0; i < outputColumns.size() && i < colNames.size(); i++) {
            String colName = colNames.get(i).toLowerCase();
            if (!keyColumnNames.contains(colName)) {
                result.add(outputColumns.get(i));
            }
        }
        return result;
    }

    /**
     * Build the column schema in the same order as outputColumns for UPDATE.
     * For partial update: key columns + assigned columns, in full schema order.
     */
    private List<Column> buildUpdateSchema(Table targetTable, UpdateStmt updateStmt) {
        List<Column> result = new ArrayList<>();
        for (Column column : targetTable.getFullSchema()) {
            if (updateStmt.usePartialUpdate() && !column.isGeneratedColumn() &&
                    !updateStmt.isAssignmentColumn(column.getName()) && !column.isKey()) {
                continue;
            }
            result.add(column);
        }
        return result;
    }
}
