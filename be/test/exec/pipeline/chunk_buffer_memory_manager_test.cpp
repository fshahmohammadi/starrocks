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

#include "exec/chunk_buffer_memory_manager.h"

#include <gtest/gtest.h>

#include "common/config.h"

namespace starrocks::pipeline {

class ChunkBufferMemoryManagerTest : public ::testing::Test {};

// Verify that max memory = max_input_dop × per_driver_limit, and is_full() triggers at threshold.
TEST_F(ChunkBufferMemoryManagerTest, test_basic_memory_limit) {
    const size_t dop = 4;
    const int64_t per_driver_limit = 128 * 1024 * 1024L; // 128MB
    ChunkBufferMemoryManager mgr(dop, per_driver_limit);

    // Total limit = 4 × 128MB = 512MB
    const int64_t total_limit = dop * per_driver_limit;

    EXPECT_FALSE(mgr.is_full());
    EXPECT_EQ(mgr.get_memory_usage(), 0);
    EXPECT_EQ(mgr.get_memory_limit_per_driver(), per_driver_limit);
    EXPECT_EQ(mgr.get_max_input_dop(), dop);

    // Add memory just below the limit — should not be full.
    mgr.update_memory_usage(total_limit - 1, 0);
    EXPECT_FALSE(mgr.is_full());

    // Push to exactly the limit — should be full (>=).
    mgr.update_memory_usage(1, 0);
    EXPECT_TRUE(mgr.is_full());
}

// Verify that is_full() returns false after memory is released.
TEST_F(ChunkBufferMemoryManagerTest, test_backpressure_release) {
    const size_t dop = 2;
    const int64_t per_driver_limit = 100;
    ChunkBufferMemoryManager mgr(dop, per_driver_limit);

    // Total limit = 200
    mgr.update_memory_usage(200, 0);
    EXPECT_TRUE(mgr.is_full());

    // Release half — should no longer be full.
    mgr.update_memory_usage(-100, 0);
    EXPECT_FALSE(mgr.is_full());
}

// Simulate the UNION ALL capping scenario: 5 branches × DOP 16 = 80 total producer DOP,
// but consumer DOP = 16, so buffer should be sized at min(80, 16) = 16 drivers.
TEST_F(ChunkBufferMemoryManagerTest, test_union_gather_capped_sizing) {
    const size_t total_producer_dop = 80; // 5 branches × 16
    const size_t consumer_dop = 16;
    const int64_t per_driver_limit = 128 * 1024 * 1024L; // 128MB

    // Capped DOP = min(80, 16) = 16
    size_t capped_dop = std::min(total_producer_dop, consumer_dop);
    EXPECT_EQ(capped_dop, 16u);

    ChunkBufferMemoryManager mgr(capped_dop, per_driver_limit);

    // Buffer limit = 16 × 128MB = 2GB, not 80 × 128MB = 10GB
    const int64_t expected_limit = 16L * 128 * 1024 * 1024; // 2GB
    const int64_t oversized_limit = 80L * 128 * 1024 * 1024; // 10GB

    // Should be full at 2GB, not 10GB.
    mgr.update_memory_usage(expected_limit, 0);
    EXPECT_TRUE(mgr.is_full());

    // Verify an uncapped manager would NOT be full at 2GB.
    ChunkBufferMemoryManager uncapped_mgr(total_producer_dop, per_driver_limit);
    uncapped_mgr.update_memory_usage(expected_limit, 0);
    EXPECT_FALSE(uncapped_mgr.is_full());

    // But would be full at 10GB.
    uncapped_mgr.update_memory_usage(oversized_limit - expected_limit, 0);
    EXPECT_TRUE(uncapped_mgr.is_full());
}

// Without capping, buffer uses the full summed DOP.
TEST_F(ChunkBufferMemoryManagerTest, test_flag_disabled_preserves_old_behavior) {
    const size_t total_producer_dop = 80;
    const int64_t per_driver_limit = 128 * 1024 * 1024L;
    ChunkBufferMemoryManager mgr(total_producer_dop, per_driver_limit);

    // Total limit = 80 × 128MB = 10GB
    const int64_t limit_2gb = 16L * 128 * 1024 * 1024;

    // At 2GB the uncapped manager should NOT be full.
    mgr.update_memory_usage(limit_2gb, 0);
    EXPECT_FALSE(mgr.is_full());
    EXPECT_EQ(mgr.get_max_input_dop(), total_producer_dop);
}

// Peak memory and peak rows track correctly.
TEST_F(ChunkBufferMemoryManagerTest, test_peak_memory_tracking) {
    const size_t dop = 4;
    const int64_t per_driver_limit = 1024;
    ChunkBufferMemoryManager mgr(dop, per_driver_limit);

    EXPECT_EQ(mgr.get_peak_memory_usage(), 0);
    EXPECT_EQ(mgr.get_peak_num_rows(), 0);

    mgr.update_memory_usage(500, 100);
    EXPECT_EQ(mgr.get_peak_memory_usage(), 500);
    EXPECT_EQ(mgr.get_peak_num_rows(), 100);

    mgr.update_memory_usage(300, 50);
    EXPECT_EQ(mgr.get_peak_memory_usage(), 800);
    EXPECT_EQ(mgr.get_peak_num_rows(), 150);

    // Release memory — peak should remain at the high-water mark.
    mgr.update_memory_usage(-600, -120);
    EXPECT_EQ(mgr.get_memory_usage(), 200);
    EXPECT_EQ(mgr.get_peak_memory_usage(), 800);
    EXPECT_EQ(mgr.get_peak_num_rows(), 150);
}

} // namespace starrocks::pipeline
