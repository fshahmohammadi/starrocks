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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared utilities for dict passthrough sink support in InsertPlanner and UpdatePlanner.
 */
public class DictPassthroughPlannerUtil {

    /**
     * Compute output column refs that are safe to keep dict-encoded for sink passthrough.
     * These are non-key columns on the target OlapTable.
     */
    public static List<ColumnRefOperator> computeSinkCandidatePassthroughColumns(
            SessionVariable sessionVariable, Table targetTable,
            List<ColumnRefOperator> outputColumns, List<String> columnNames) {
        if (!sessionVariable.isEnableDictPassthroughSink()) {
            return List.of();
        }
        if (!(targetTable instanceof OlapTable olapTable)) {
            return List.of();
        }
        if (targetTable.isCloudNativeTableOrMaterializedView()) {
            return List.of();
        }
        Set<String> keyColumnNames = getKeyColumnNames(olapTable);
        List<ColumnRefOperator> result = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!keyColumnNames.contains(columnNames.get(i).toLowerCase())
                    && !outputColumns.get(i).getType().isStructType()) {
                result.add(outputColumns.get(i));
            }
        }
        return result;
    }

    /**
     * Returns the set of key/partition/distribution/sort column names (lowercased)
     * for an OlapTable. Used to identify columns that must NOT be passthrough candidates.
     */
    static Set<String> getKeyColumnNames(OlapTable olapTable) {
        Set<String> keyColumnNames = new HashSet<>();
        keyColumnNames.addAll(olapTable.getPartitionColumnNames().stream()
                .map(String::toLowerCase).collect(Collectors.toList()));
        keyColumnNames.addAll(olapTable.getDistributionColumnNames());
        keyColumnNames.addAll(olapTable.getKeyColumns().stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toList()));
        MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
        if (indexMeta != null && indexMeta.getSortKeyIdxes() != null) {
            List<Column> schema = indexMeta.getSchema();
            keyColumnNames.addAll(indexMeta.getSortKeyIdxes().stream()
                    .filter(idx -> idx < schema.size())
                    .map(idx -> schema.get(idx).getName().toLowerCase())
                    .collect(Collectors.toList()));
        }
        return keyColumnNames;
    }

    /**
     * Wire dict passthrough onto the sink and fragment. Shared by InsertPlanner and UpdatePlanner.
     */
    public static void wireDictPassthrough(Map<Integer, Integer> passthroughSourceSlotMap,
                                     Collection<String> passthroughColumnNames,
                                     DataSink dataSink, PlanFragment sinkFragment, ExecPlan execPlan) {
        if (passthroughSourceSlotMap.isEmpty()) {
            return;
        }
        Preconditions.checkState(dataSink instanceof OlapTableSink);
        ((OlapTableSink) dataSink).setDictPassthroughColumnNames(passthroughColumnNames);
        sinkFragment.setDictPassthroughSourceSlotMap(passthroughSourceSlotMap);
        // Propagate dicts and dict exprs from the last fragment to the sink fragment.
        PlanFragment lastFragment = execPlan.getFragments().get(execPlan.getFragments().size() - 1);
        sinkFragment.mergeQueryGlobalDicts(lastFragment.getQueryGlobalDicts());
        if (lastFragment.getQueryGlobalDictExprs() != null) {
            sinkFragment.mergeQueryDictExprs(lastFragment.getQueryGlobalDictExprs());
        }
    }

    /**
     * Read passthrough result from the optimizer and swap passthrough columns to their dict refs
     * in the output column list. Populates passthroughColumnToDictRefSlotId for slot descriptor setup.
     */
    public static List<ColumnRefOperator> buildEffectiveOutputColumns(
            Map<Integer, Integer> passthroughResult,
            List<ColumnRefOperator> outputColumns,
            List<Column> schema,
            ColumnRefFactory columnRefFactory,
            Map<String, Integer> passthroughColumnToDictRefSlotId) {
        List<ColumnRefOperator> effectiveOutputColumns = new ArrayList<>(outputColumns);
        if (!passthroughResult.isEmpty()) {
            for (int i = 0; i < outputColumns.size() && i < schema.size(); i++) {
                Integer dictRefId = passthroughResult.get(outputColumns.get(i).getId());
                if (dictRefId != null) {
                    ColumnRefOperator dictRefCol = columnRefFactory.getColumnRef(dictRefId);
                    passthroughColumnToDictRefSlotId.put(schema.get(i).getName(), dictRefId);
                    effectiveOutputColumns.set(i, dictRefCol);
                }
            }
        }
        return effectiveOutputColumns;
    }
}
