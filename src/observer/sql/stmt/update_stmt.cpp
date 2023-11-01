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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"

#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, const std::vector<Value>& values, int value_amount,FilterStmt* filter_st,const std::vector<std::string>& attrname)
    : table_(table), values_(values), value_amount_(value_amount),filter_stmt_(filter_st),attr_names_(attrname)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{

  // TODO
  stmt = nullptr;
  const char *table_name = update.relation_name.c_str();
  // Value value=update.values;
  // std::string attr_name=update.attribute_names;
  std::vector<ConditionSqlNode> conditions=update.conditions;

  for(auto v: update.values)
  {
    if(!v.value_is_vaild())
      return RC::INVALID_ARGUMENT;
  }
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%s",
        db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }


  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // check whether the field exists
  const TableMeta &table_meta = table->table_meta();
  for(int i=0;i<update.attribute_names.size();i++)
  {
    const std::string& s=update.attribute_names[i];
    const Value& v=update.values[i];
    const FieldMeta *field_meta=nullptr;
    if((field_meta=table_meta.field(s.c_str()))==nullptr)
    {
      return RC::INVALID_ARGUMENT;
    }
    const AttrType field_type = field_meta->type();
    const AttrType value_type = v.attr_type();
    if (field_type != value_type) {  // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
      table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }
  stmt = new UpdateStmt(table, update.values,update.values.size(),filter_stmt,update.attribute_names);
  return RC::SUCCESS;
  // const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  // const int sys_field_num = table_meta.sys_field_num();

  // for (int i = 0; i < field_num; i++) {
  //   const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    
    
  //   //如果字段名和属性名一致
  //   if(strcmp(attr_name.c_str(),field_meta->name())==0) {
  //     const AttrType field_type = field_meta->type();
  //     const AttrType value_type = value.attr_type();
  //     if (field_type != value_type) {  // TODO try to convert the value type to field type
  //       LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
  //       table_name, field_meta->name(), field_type, value_type);
  //       return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  //     }
  //     stmt = new UpdateStmt(table, value, 1,filter_stmt,attr_name);
  //     return RC::SUCCESS;
  //   }
   
  // }
  

  //return RC::INVALID_ARGUMENT;

}
