/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");

RC IndexMeta::init(const char *name, std::vector<const FieldMeta *>&field) // todo indexmeta.h
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  for(auto x:field)
    field_.push_back(x->name());
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  json_value.clear();
  for(auto x:field_)
    json_value[FIELD_FIELD_NAME].append(x);
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if ((!field_value.isArray())&&(!field_value.isString())) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(),
        field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  std::vector<const FieldMeta *> fields;

  for(auto it=field_value.begin();it!=field_value.end();it++)
  {
    if(!(it->isString()))
    {
      return RC::INTERNAL;
    }
    const char* str = it->asCString();
    const FieldMeta *field = table.field(str);
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(field);
  }
  // std::vector<const FieldMeta *> fields;
  // const char* str=field_value.asCString();
  // int pos=0;std::string tmp;
  // while(str[pos])
  // {
  //   if(str[pos]!='+')
  //     tmp.push_back(str[pos]);
  //   else {
  //     const FieldMeta *field = table.field(tmp.c_str());
  //     if (nullptr == field) {
  //       LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
  //       return RC::SCHEMA_FIELD_MISSING;
  //     }
  //     fields.push_back(field);
  //     tmp.clear();
  //   }
  //   pos++;
  // }
  // const FieldMeta *field = table.field(tmp.c_str());
  // if (nullptr == field) {
  //   LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
  //   return RC::SCHEMA_FIELD_MISSING;
  // }
  // fields.push_back(field);
  return index.init(name_value.asCString(),fields);
}

const char *IndexMeta::name() const
{
  return name_.c_str();
}

std::vector<std::string> IndexMeta::field() const
{
  return field_;
}

void IndexMeta::desc(std::ostream &os) const
{
  std::string field_str;
  for(auto x:field_)
  {
    field_str+="+";
    field_str+=x;
  }
  field_str.pop_back();
  os << "index name=" << name_ << ", field=" << field_str;
}