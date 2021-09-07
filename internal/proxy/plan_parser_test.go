// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"fmt"
	"testing"

	ant_parser "github.com/antonmedv/expr/parser"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func newTestSchema() *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		if !typeutil.IsIntegerType(dataType) && !typeutil.IsFloatingType(dataType) && !typeutil.IsVectorType(dataType) {
			continue
		}
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		fields = append(fields, newField)
	}

	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      true,
		Fields:      fields,
	}
}

func TestParseQueryExpr_Naive(t *testing.T) {
	exprStr := "Int64Field > 3"
	schemaPb := newTestSchema()
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	assert.Nil(t, err)
	exprProto, err := parseQueryExpr(schema, exprStr)
	assert.Nil(t, err)
	str := proto.MarshalTextString(exprProto)
	println(str)
}

func TestParsePlanNode_Naive(t *testing.T) {
	exprStrs := []string{
		"not (Int64Field > 3)",
		"not (3 > Int64Field)",
		"Int64Field in [1, 2, 3]",
		"Int64Field < 3 and (Int64Field > 2 || Int64Field == 1)",
		"DoubleField in [1.0, 2, 3]",
		"DoubleField in [1.0, 2, 3] && Int64Field < 3 or Int64Field > 2",
	}

	schema := newTestSchema()
	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	// Note: use pointer to string to represent nullable string
	// TODO: change it to better solution
	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := CreateQueryPlan(schema, exprStr, "FloatVectorField", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}

}

func TestExternalParser(t *testing.T) {
	ast, err := ant_parser.Parse("!(1 < a < 2 or b in [1, 2, 3]) or (c < 3 and b > 5)")
	// NOTE: probe ast here via IDE
	assert.Nil(t, err)

	println(ast.Node.Location().Column)
}

func TestExprPlan_Str(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	// without filter
	planProto, err := CreateQueryPlan(schema, "", "fakevec", queryInfo)
	assert.Nil(t, err)
	dbgStr := proto.MarshalTextString(planProto)
	println(dbgStr)

	exprStrs := []string{
		"age >= 420000 && age < 420010", // range
		"age == 420000 || age == 420001 || age == 420002 || age == 420003 || age == 420004", // term
		"age not in [1, 2, 3]",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := CreateQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestExprMultiRange_Str(t *testing.T) {
	exprStrs := []string{
		"3 < FloatN < 4.0",
		"3 < age1 < 5 < age2 < 7 < FloatN < 9.0 < FloatN2",
		"1 + 1 < age1 < 2 * 2",
		"1 - 1 < age1 < 3 / 2",
		"1.0 - 1 < FloatN < 3 / 2",
		"2 ** 10 > FloatN >= 7 % 4",
		"0.1 ** 2 < FloatN < 2 ** 0.1",
		"0.1 ** 1.1 < FloatN < 3.1 / 4",
		"4.1 / 3 < FloatN < 0.0 / 5.0",
	}

	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age1", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age2", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "FloatN", DataType: schemapb.DataType_Float},
		{FieldID: 104, Name: "FloatN2", DataType: schemapb.DataType_Float},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := CreateQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestExprFieldCompare_Str(t *testing.T) {
	exprStrs := []string{
		"age1 < age2",
		"3 < age1 <= age2 < 4",
	}

	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age1", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age2", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "FloatN", DataType: schemapb.DataType_Float},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := CreateQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}
