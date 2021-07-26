// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
// Generated File
// DO NOT EDIT
#include <optional>
#include <boost/dynamic_bitset.hpp>
#include <boost/variant.hpp>
#include <utility>
#include <deque>
#include "segcore/SegmentGrowingImpl.h"
#include "query/ExprImpl.h"
#include "ExprVisitor.h"

namespace milvus::query {
class ExecExprVisitor : public ExprVisitor {
 public:
    void
    visit(LogicalUnaryExpr& expr) override;

    void
    visit(LogicalBinaryExpr& expr) override;

    void
    visit(TermExpr& expr) override;

    void
    visit(UnaryRangeExpr& expr) override;

    void
    visit(BinaryRangeExpr& expr) override;

    void
    visit(CompareExpr& expr) override;

 public:
    using RetType = boost::dynamic_bitset<>;
    ExecExprVisitor(const segcore::SegmentInternalInterface& segment, int64_t row_count, Timestamp timestamp)
        : segment_(segment), row_count_(row_count), timestamp_(timestamp) {
    }
    RetType
    call_child(Expr& expr) {
        Assert(!ret_.has_value());
        expr.accept(*this);
        Assert(ret_.has_value());
        auto res = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(res.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(FieldOffset field_offset, IndexFunc func, ElementFunc element_func) -> RetType;

    template <typename T>
    auto
    ExecUnaryRangeVisitorDispatcher(UnaryRangeExpr& expr_raw) -> RetType;

    template <typename T>
    auto
    ExecBinaryRangeVisitorDispatcher(BinaryRangeExpr& expr_raw) -> RetType;

    template <typename T>
    auto
    ExecTermVisitorImpl(TermExpr& expr_raw) -> RetType;

    template <typename CmpFunc>
    auto
    ExecCompareExprDispatcher(CompareExpr& expr, CmpFunc cmp_func) -> RetType;

 private:
    const segcore::SegmentInternalInterface& segment_;
    int64_t row_count_;
    std::optional<RetType> ret_;
    Timestamp timestamp_;
};
}  // namespace milvus::query
