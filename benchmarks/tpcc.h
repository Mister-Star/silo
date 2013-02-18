#ifndef _NDB_BENCH_TPCC_H_
#define _NDB_BENCH_TPCC_H_

#include <stdint.h>

#include "inline_str.h"
#include "../macros.h"

namespace tpcc {

template <typename T> struct encoder {};

// abuse the C preprocessor due to laziness
// we really should auto-generate this code

#define STRUCT_LAYOUT_X(tpe, name) \
  tpe name;

#define STRUCT_EQ_X(tpe, name) \
  if (this->name != other.name) \
    return false;

#define SERIALIZE_WRITE_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    buf = s.write(buf, (const tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#define SERIALIZE_READ_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    buf = s.read(buf, (tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#define SERIALIZE_NBYTES_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    size += s.nbytes((const tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#ifdef USE_VARINT_ENCODING
#define DO_STRUCT_REST(name) \
  inline const uint8_t * \
  write(uint8_t *temp_buf, const struct name *obj) const \
  { \
    encode_write(temp_buf, obj); \
    return temp_buf; \
  } \
  inline const struct name * \
  read(const uint8_t *buf, struct name *temp_obj) const \
  { \
    encode_read(buf, temp_obj); \
    return temp_obj; \
  } \
  inline size_t \
  nbytes(const struct name *obj) const \
  { \
    return encode_nbytes(obj); \
  }
#else
#define DO_STRUCT_REST(name) \
  inline const uint8_t * \
  write(uint8_t *temp_buf, const struct name *obj) const \
  { \
    return (const uint8_t *) obj; \
  } \
  inline const struct name * \
  read(const uint8_t *buf, struct name *temp_obj) const \
  { \
    return (const struct name *) buf; \
  } \
  inline size_t \
  nbytes(const struct name *obj) const \
  { \
    return sizeof(*obj); \
  }
#endif

#define DO_STRUCT(name, fields) \
  struct name { \
    fields(STRUCT_LAYOUT_X) \
    inline bool \
    operator==(const struct name &other) const \
    { \
      fields(STRUCT_EQ_X) \
      return true; \
    } \
    inline bool \
    operator!=(const struct name &other) const \
    { \
      return !operator==(other); \
    } \
  } PACKED; \
  template <> \
  struct encoder< name > { \
  private: \
  uint8_t * \
  encode_write(uint8_t *buf, const struct name *obj) const \
  { \
    const uint8_t *field = (const uint8_t *) obj; \
    fields(SERIALIZE_WRITE_FIELD_X) \
    return buf; \
  } \
  const uint8_t * \
  encode_read(const uint8_t *buf, struct name *obj) const \
  { \
    uint8_t *field = (uint8_t *) obj; \
    fields(SERIALIZE_READ_FIELD_X) \
    return buf; \
  } \
  size_t \
  encode_nbytes(const struct name *obj) const \
  { \
    size_t size = 0; \
    const uint8_t *field = (const uint8_t *) obj; \
    fields(SERIALIZE_NBYTES_FIELD_X) \
    return size; \
  } \
  public: \
  DO_STRUCT_REST(name) \
  };

#define CUSTOMER_FIELDS(x) \
  x(int32_t,c_w_id) \
  x(int32_t,c_d_id) \
  x(int32_t,c_id) \
  x(float,c_discount) \
  x(inline_str_fixed<2>,c_credit) \
  x(inline_str_8<16>,c_last) \
  x(inline_str_8<16>,c_first) \
  x(float,c_credit_lim) \
  x(float,c_balance) \
  x(float,c_ytd_payment) \
  x(int32_t,c_payment_cnt) \
  x(int32_t,c_delivery_cnt) \
  x(inline_str_8<20>,c_street_1) \
  x(inline_str_8<20>,c_street_2) \
  x(inline_str_8<20>,c_city) \
  x(inline_str_fixed<2>,c_state) \
  x(inline_str_fixed<9>,c_zip) \
  x(inline_str_fixed<16>,c_phone) \
  x(uint32_t,c_since) \
  x(inline_str_fixed<2>,c_middle) \
  x(inline_str_16<500>,c_data)
DO_STRUCT(customer, CUSTOMER_FIELDS)

#define CUSTOMER_NAME_IDX_MEM_FIELDS(x) \
	x(int32_t,c_id) \
	x(intptr_t,c_ptr)
DO_STRUCT(customer_name_idx_mem, CUSTOMER_NAME_IDX_MEM_FIELDS)

#define CUSTOMER_NAME_IDX_NOMEM_FIELDS(x) \
	x(int32_t,c_id)
DO_STRUCT(customer_name_idx_nomem, CUSTOMER_NAME_IDX_NOMEM_FIELDS)

#define DISTRICT_FIELDS(x) \
  x(int32_t,d_w_id) \
  x(int32_t,d_id) \
  x(float,d_ytd) \
  x(float,d_tax) \
  x(int32_t,d_next_o_id) \
  x(inline_str_8<10>,d_name) \
  x(inline_str_8<20>,d_street_1) \
  x(inline_str_8<20>,d_street_2) \
  x(inline_str_8<20>,d_city) \
  x(inline_str_fixed<2>,d_state) \
  x(inline_str_fixed<9>,d_zip)
DO_STRUCT(district, DISTRICT_FIELDS)

#define HISTORY_FIELDS(x) \
  x(int32_t,h_c_id) \
  x(int32_t,h_c_d_id) \
  x(int32_t,h_c_w_id) \
  x(int32_t,h_d_id) \
  x(int32_t,h_w_id) \
  x(uint32_t,h_date) \
  x(float,h_amount) \
  x(inline_str_8<24>,h_data)
DO_STRUCT(history, HISTORY_FIELDS)

#define ITEM_FIELDS(x) \
  x(int32_t,i_id) \
  x(inline_str_8<24>,i_name) \
  x(float,i_price) \
  x(inline_str_8<50>,i_data) \
  x(int32_t,i_im_id)
DO_STRUCT(item, ITEM_FIELDS)

#define NEW_ORDER_FIELDS(x) \
  x(int32_t,no_w_id) \
  x(int32_t,no_d_id) \
  x(int32_t,no_o_id)
DO_STRUCT(new_order, NEW_ORDER_FIELDS)

#define OORDER_FIELDS(x) \
  x(int32_t,o_w_id) \
  x(int32_t,o_d_id) \
  x(int32_t,o_id) \
  x(int32_t,o_c_id) \
  x(int32_t,o_carrier_id) \
  x(int8_t,o_ol_cnt) \
  x(bool,o_all_local) \
  x(uint32_t,o_entry_d)
DO_STRUCT(oorder, OORDER_FIELDS)

#define OORDER_C_ID_IDX_MEM_FIELDS(x) \
	x(int32_t,o_id) \
	x(intptr_t,o_ptr)
DO_STRUCT(oorder_c_id_idx_mem, OORDER_C_ID_IDX_MEM_FIELDS)

#define OORDER_C_ID_IDX_NOMEM_FIELDS(x) \
	x(int32_t,o_id)
DO_STRUCT(oorder_c_id_idx_nomem, OORDER_C_ID_IDX_NOMEM_FIELDS)

#define ORDER_LINE_FIELDS(x) \
  x(int32_t,ol_w_id) \
  x(int32_t,ol_d_id) \
  x(int32_t,ol_o_id) \
  x(int32_t,ol_number) \
  x(int32_t,ol_i_id) \
  x(uint32_t,ol_delivery_d) \
  x(float,ol_amount) \
  x(int32_t,ol_supply_w_id) \
  x(int8_t,ol_quantity) \
  x(inline_str_fixed<24>,ol_dist_info)
DO_STRUCT(order_line, ORDER_LINE_FIELDS)

#define STOCK_FIELDS(x) \
  x(int32_t,s_w_id) \
  x(int32_t,s_i_id) \
  x(int16_t,s_quantity) \
  x(float,s_ytd) \
  x(int32_t,s_order_cnt) \
  x(int32_t,s_remote_cnt) \
  x(inline_str_8<50>,s_data) \
  x(inline_str_fixed<24>,s_dist_01) \
  x(inline_str_fixed<24>,s_dist_02) \
  x(inline_str_fixed<24>,s_dist_03) \
  x(inline_str_fixed<24>,s_dist_04) \
  x(inline_str_fixed<24>,s_dist_05) \
  x(inline_str_fixed<24>,s_dist_06) \
  x(inline_str_fixed<24>,s_dist_07) \
  x(inline_str_fixed<24>,s_dist_08) \
  x(inline_str_fixed<24>,s_dist_09) \
  x(inline_str_fixed<24>,s_dist_10)
DO_STRUCT(stock, STOCK_FIELDS)

#define WAREHOUSE_FIELDS(x) \
  x(int32_t,w_id) \
  x(float,w_ytd) \
  x(float,w_tax) \
  x(inline_str_8<10>,w_name) \
  x(inline_str_8<20>,w_street_1) \
  x(inline_str_8<20>,w_street_2) \
  x(inline_str_8<20>,w_city) \
  x(inline_str_fixed<2>,w_state) \
  x(inline_str_fixed<9>,w_zip)
DO_STRUCT(warehouse, WAREHOUSE_FIELDS)

}

#endif
