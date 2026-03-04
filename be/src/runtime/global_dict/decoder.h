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

#pragma once

#include "column/column.h"
#include "common/status.h"
#include "runtime/global_dict/types_fwd_decl.h"

namespace starrocks {

// Decode a dict-encoded column (Int32 codes) to a string column (BinaryColumn).
// Uses batch append_strings() for efficiency.
// Handles both nullable and non-nullable input.
ColumnPtr decode_dict_column_to_string(const Column& dict_column, const RGlobalDictMap& reverse_dict);

class GlobalDictDecoder {
public:
    virtual ~GlobalDictDecoder() = default;

    virtual Status decode_string(const Column* in, Column* out) = 0;

    virtual Status decode_array(const Column* in, Column* out) = 0;
};

using GlobalDictDecoderPtr = std::unique_ptr<GlobalDictDecoder>;

template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict);

} // namespace starrocks
