#include "keys.h"

namespace arki {
namespace structured {

static Keys make_json()
{
    Keys res;
    res.type_name = "t";
    res.type_desc = "desc";
    res.type_style = "s";
    res.reftime_position_time = "ti";
    res.reftime_period_begin = "b";
    res.reftime_period_end = "e";
    res.origin_centre = "ce";
    res.origin_subcentre = "sc";
    res.origin_process = "pr";
    res.origin_process_type = "pt";
    res.origin_background_process_id = "bi";
    res.origin_process_id = "pi";
    res.origin_wmo = "wmo";
    res.origin_rad = "rad";
    res.origin_plc = "plc";
    res.product_origin = "or";
    res.product_table = "ta";
    res.product_product = "pr";
    res.product_centre = "ce";
    res.product_discipline = "di";
    res.product_category = "ca";
    res.product_number = "no";
    res.product_table_version = "tv";
    res.product_local_table_version = "ltv";
    res.product_type = "ty";
    res.product_subtype = "st";
    res.product_local_subtype = "ls";
    res.product_value = "va";
    res.product_object = "ob";
    res.product_id = "id";
    res.source_format = "f";
    res.source_size = "sz";
    res.source_url = "url";
    res.source_basedir = "b";
    res.source_file = "file";
    res.source_offset = "ofs";
    res.level_type = "lt";
    res.level_scale = "sc";
    res.level_scale1 = "s1";
    res.level_scale2 = "s2";
    res.level_value = "va";
    res.level_value1 = "v1";
    res.level_value2 = "v2";
    res.level_l1 = "l1";
    res.level_l2 = "l2";
    res.level_min = "mi";
    res.level_max = "ma";
    res.timerange_type = "ty";
    res.timerange_unit = "un";
    res.timerange_p1 = "p1";
    res.timerange_p2 = "p2";
    res.timerange_value = "va";
    res.timerange_step_len = "sl";
    res.timerange_step_unit = "su";
    res.timerange_stat_type = "pt";
    res.timerange_stat_len = "pl";
    res.timerange_stat_unit = "pu";
    res.area_id = "id";
    res.area_value = "va";
    res.proddef_value = "va";
    res.value_value = "va";
    res.run_value = "va";
    res.quantity_value = "va";
    res.task_value = "va";
    res.assigneddataset_time = "ti";
    res.assigneddataset_name = "na";
    res.assigneddataset_id = "id";
    res.note_time = "ti";
    res.note_value = "va";
    res.metadata_items = "i";
    res.metadata_notes = "n";
    res.summary_items = "items";
    res.summary_stats = "summarystats";
    res.summary_desc = "desc";
    res.summarystats_begin = "b";
    res.summarystats_end = "e";
    res.summarystats_count = "c";
    res.summarystats_size = "s";
    return res;
}

static Keys make_python()
{
    Keys res;
    res.type_name = "type";
    res.type_desc = "desc";
    res.type_style = "style";
    res.reftime_position_time = "time";
    res.reftime_period_begin = "begin";
    res.reftime_period_end = "end";
    res.origin_centre = "centre";
    res.origin_subcentre = "subcentre";
    res.origin_process = "process";
    res.origin_process_type = "process_type";
    res.origin_background_process_id = "background_process_id";
    res.origin_process_id = "process_id";
    res.origin_wmo = "wmo";
    res.origin_rad = "rad";
    res.origin_plc = "plc";
    res.product_origin = "origin";
    res.product_table = "table";
    res.product_product = "product";
    res.product_centre = "centre";
    res.product_discipline = "discipline";
    res.product_category = "category";
    res.product_number = "number";
    res.product_table_version = "table_version";
    res.product_local_table_version = "local_table_version";
    res.product_type = "type";
    res.product_subtype = "subtype";
    res.product_local_subtype = "local_subtype";
    res.product_value = "value";
    res.product_object = "object";
    res.product_id = "id";
    res.source_format = "format";
    res.source_size = "size";
    res.source_url = "url";
    res.source_basedir = "basedir";
    res.source_file = "file";
    res.source_offset = "offset";
    res.level_type = "level_type";
    res.level_scale = "scale";
    res.level_scale1 = "scale1";
    res.level_scale2 = "scale2";
    res.level_value = "value";
    res.level_value1 = "value1";
    res.level_value2 = "value2";
    res.level_l1 = "l1";
    res.level_l2 = "l2";
    res.level_min = "min";
    res.level_max = "max";
    res.timerange_type = "trange_type";
    res.timerange_unit = "unit";
    res.timerange_p1 = "p1";
    res.timerange_p2 = "p2";
    res.timerange_value = "value";
    res.timerange_step_len = "step_len";
    res.timerange_step_unit = "step_unit";
    res.timerange_stat_type = "stat_type";
    res.timerange_stat_len = "stat_len";
    res.timerange_stat_unit = "stat_unit";
    res.area_id = "id";
    res.area_value = "value";
    res.proddef_value = "value";
    res.value_value = "value";
    res.run_value = "value";
    res.quantity_value = "value";
    res.task_value = "value";
    res.assigneddataset_time = "time";
    res.assigneddataset_name = "name";
    res.assigneddataset_id = "id";
    res.note_time = "time";
    res.note_value = "value";
    res.metadata_items = "items";
    res.metadata_notes = "notes";
    res.summary_items = "items";
    res.summary_stats = "summarystats";
    res.summary_desc = "desc";
    res.summarystats_begin = "begin";
    res.summarystats_end = "end";
    res.summarystats_count = "count";
    res.summarystats_size = "size";
    return res;
}


const Keys keys_json = make_json();
const Keys keys_python = make_python();

}
}
