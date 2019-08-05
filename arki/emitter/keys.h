#ifndef ARKI_EMITTER_KEYS_H
#define ARKI_EMITTER_KEYS_H

namespace arki {
namespace emitter {

struct Keys
{
    const char* type_name;
    const char* type_desc;
    const char* type_style;
    const char* reftime_position_time;
    const char* reftime_period_begin;
    const char* reftime_period_end;
    const char* source_format;
    const char* source_size;
    const char* source_url;
    const char* source_basedir;
    const char* source_file;
    const char* source_offset;
    const char* origin_centre;
    const char* origin_subcentre;
    const char* origin_process;
    const char* origin_process_type;
    const char* origin_background_process_id;
    const char* origin_process_id;
    const char* origin_wmo;
    const char* origin_rad;
    const char* origin_plc;
    const char* product_origin;
    const char* product_table;
    const char* product_product;
    const char* product_centre;
    const char* product_discipline;
    const char* product_category;
    const char* product_number;
    const char* product_table_version;
    const char* product_local_table_version;
    const char* product_type;
    const char* product_subtype;
    const char* product_local_subtype;
    const char* product_value;
    const char* product_object;
    const char* product_id;
    const char* level_type;
    const char* level_scale;
    const char* level_scale1;
    const char* level_scale2;
    const char* level_value;
    const char* level_value1;
    const char* level_value2;
    const char* level_l1;
    const char* level_l2;
    const char* level_min;
    const char* level_max;
    const char* timerange_type;
    const char* timerange_unit;
    const char* timerange_p1;
    const char* timerange_p2;
    const char* timerange_value;
    const char* timerange_step_len;
    const char* timerange_step_unit;
    const char* timerange_stat_type;
    const char* timerange_stat_len;
    const char* timerange_stat_unit;
    const char* area_id;
    const char* area_value;
    const char* proddef_value;
    const char* value_value;
    const char* run_value;
    const char* quantity_value;
    const char* task_value;
    const char* assigneddataset_time;
    const char* assigneddataset_name;
    const char* assigneddataset_id;
    const char* note_time;
    const char* note_value;
};

extern const Keys keys_json;
extern const Keys keys_python;

}
}

#endif
