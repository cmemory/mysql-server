/* Copyright (c) 2015, 2021, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/dd/dd_tablespace.h"

#include <stddef.h>
#include <string.h>
#include <memory>

#include "lex_string.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_sys.h"
#include "mysql_com.h"
#include "mysqld_error.h"

#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd.h"                       // dd::create_object
#include "sql/dd/dictionary.h"               // dd::Dictionary::is_dd_table...
#include "sql/dd/impl/dictionary_impl.h"     // dd::dd_tablespace_id()
#include "sql/dd/impl/system_registry.h"     // dd::System_tablespaces
#include "sql/dd/object_id.h"
#include "sql/dd/properties.h"  // dd::Properties
#include "sql/dd/string_type.h"
#include "sql/dd/types/index.h"            // dd::Index
#include "sql/dd/types/partition.h"        // dd::Partition
#include "sql/dd/types/partition_index.h"  // dd::Partition_index
#include "sql/dd/types/table.h"            // dd::Table
#include "sql/dd/types/tablespace.h"       // dd::Tablespace
#include "sql/dd/types/tablespace_file.h"  // dd::Tablespace_file
#include "sql/key.h"
#include "sql/sql_class.h"  // THD
#include "sql/sql_servers.h"
#include "sql/sql_table.h"  // validate_comment_length
#include "sql/table.h"

namespace {
template <typename T>
bool get_and_store_tablespace_name(THD *thd, const T *obj,
                                   Tablespace_hash_set *tablespace_set) {
  const char *tablespace_name = nullptr;
  // 根据不同情况获取tablespace name
  if (get_tablespace_name(thd, obj, &tablespace_name, thd->mem_root)) {
    return true;
  }

  // 将获取的tablespace name加入到tablespace_set中
  if (tablespace_name) {
    tablespace_set->insert(tablespace_name);
  }

  return false;
}

}  // anonymous namespace

namespace dd {

bool fill_table_and_parts_tablespace_names(
    THD *thd, const char *db_name, const char *table_name,
    Tablespace_hash_set *tablespace_set) {
  // Get hold of the dd::Table object.
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table_obj = nullptr;
  // 根据db_name和table_name从DD中获取table信息，存入table_obj。
  if (thd->dd_client()->acquire(db_name, table_name, &table_obj)) {
    // Error is reported by the dictionary subsystem.
    return true;
  }

  if (table_obj == nullptr) {
    /*
      A non-existing table is a perfectly valid scenario, e.g. for
      statements using the 'IF EXISTS' clause. Thus, we cannot throw
      an error in this situation, we just continue returning succuss.
    */
    // table不存在的可能是我们预期的，如指定IF EXISTS从句时。所以不能直接报错，还是要返回成功
    return false;
  }

  // Add the tablespace name used by dd::Table.
  // 根据table_obj获取tablespace name并放入tablespace_set
  if (get_and_store_tablespace_name(thd, table_obj, tablespace_set)) {
    return true;
  }

  /*
    Add tablespaces used by partition/subpartition definitions.
    Note that dd::Table::partitions() gives use both partitions
    and sub-partitions.
    dd::Table::partitions()返回了分区和子分区，后面为什么还循环遍历子分区？
   */
  // 表有分区的话，迭代各个分区，将分区定义中的tablespace names加入到tablespace_set中
  if (table_obj->partition_type() != dd::Table::PT_NONE) {
    // Iterate through tablespace names used by partitions/indexes.
    // 迭代表的partitions处理
    for (const dd::Partition *part_obj : table_obj->partitions()) {
        // 处理part_obj所在的tablespace name
      if (get_and_store_tablespace_name(thd, part_obj, tablespace_set))
        return true;

        // 循环迭代处理part_obj所使用的indexes所在的tablespace name
      for (const dd::Partition_index *part_idx_obj : part_obj->indexes())
        if (get_and_store_tablespace_name(thd, part_idx_obj, tablespace_set))
          return true;

      // Iterate through tablespace names used by subpartition/indexes.
      // 循环迭代part_obj的子分区
      for (const dd::Partition *sub_part_obj : part_obj->subpartitions()) {
          // 处理子分区所在的tablespace name
        if (get_and_store_tablespace_name(thd, sub_part_obj, tablespace_set))
          return true;

        // 循环迭代子分区sub_part_obj所使用的indexes所在的tablespace name
        for (const dd::Partition_index *subpart_idx_obj :
             sub_part_obj->indexes())
          if (get_and_store_tablespace_name(thd, subpart_idx_obj,
                                            tablespace_set))
            return true;
      }
    }
  }

  // Add tablespaces used by indexes.
  // 添加表所使用的索引锁在的表空间name
  for (const dd::Index *idx_obj : table_obj->indexes())
    if (get_and_store_tablespace_name(thd, idx_obj, tablespace_set))
      return true;
  // TODO WL#7156: Add tablespaces used by individual columnns.

  return false;
}

template <typename T>
bool get_tablespace_name(THD *thd, const T *obj, const char **tablespace_name,
                         MEM_ROOT *mem_root) {
  //
  // Read Tablespace
  //
  String_type name;

  if (obj->tablespace_id() == Dictionary_impl::dd_tablespace_id()) {
    // If this is the DD tablespace id, then we use its name.
    // 表位于系统表空间，tablespace name直接获取
    name = MYSQL_TABLESPACE_NAME.str;
  } else if (obj->tablespace_id() != dd::INVALID_OBJECT_ID) {
    /*
      We get here, when we have a table in a tablespace
      which is 'innodb_system' or a user defined tablespace.

      We cannot take MDL lock as we don't know the tablespace name.
      Without a MDL lock we cannot acquire a object placing it in DD
      cache. So we are acquiring the object uncached.

      进入这里是因为DD有效，但取得的对象不属于当前系统DD，对象可能是innodb_system或用户自定义的表空间。
      我们无法拿到MDL锁，因为我们不知道对象对应的表空间name，没有MDL锁就无法将获取对象放到DD缓存中。所以获取表空间对象但不进行缓存。

      注意到，理论上有一个事实是我们打开一个表在某表空间，意味着这个表空间不能同时被dropped或created。
      所以理论上我们可以在这个表空间上我们可以完全的获取意向共享锁。这点与对schemas的处理类似。

      Note that in theory the fact that we are opening a table in
      some tablespace means that this tablespace can't be dropped
      or created concurrently, so in theory we hold implicit IS
      lock on tablespace (similarly to how it happens for schemas).
    */
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    dd::Tablespace *tablespace = nullptr;
    // 查到对象对应的tablespace
    if (thd->dd_client()->acquire_uncached(obj->tablespace_id(), &tablespace)) {
      // acquire() always fails with a error being reported.
      return true;
    }

    // Report error if tablespace not found.
    if (!tablespace) {
      my_error(ER_INVALID_DD_OBJECT_ID, MYF(0), obj->tablespace_id());
      return true;
    }

    // 获取tablespace name
    name = tablespace->name();
  } else {
    /*
      If user has specified special tablespace name like
      'innodb_file_per_table' then we read it from tablespace options.
    */
    // 如果用户指定了特定的表空间，则可以直接从表的options中获取tablespace name
    if (obj->options().exists("tablespace"))
      (void)obj->options().get("tablespace", &name);
  }

  *tablespace_name = nullptr;
  if (!name.empty() && !(*tablespace_name = strmake_root(mem_root, name.c_str(),
                                                         name.length()))) {
    return true;
  }

  return false;
}

// The explicit instantiation of the template members below
// is not handled well by doxygen, so we enclose this in a
// cond/endcon block.

/**
 @cond
*/

template bool get_tablespace_name<dd::Partition>(THD *, dd::Partition const *,
                                                 char const **, MEM_ROOT *);
template bool get_tablespace_name<dd::Partition_index>(
    THD *, dd::Partition_index const *, char const **, MEM_ROOT *);
template bool get_tablespace_name<dd::Index>(THD *, dd::Index const *,
                                             char const **, MEM_ROOT *);
template bool get_tablespace_name<dd::Table>(THD *, dd::Table const *,
                                             char const **, MEM_ROOT *);

/**
 @endcond
*/

}  // namespace dd
