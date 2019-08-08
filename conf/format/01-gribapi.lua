-- GRIBAPI_DEF_DIR is the list of directories (separated by :) where grib_api/eccodes keeps its definitions
for _, name in pairs({"ECCODES_DEFINITION_PATH", "GRIBAPI_DEFINITION_PATH"})
do
    GRIBAPI_DEF_DIR = os.getenv(name)
    if GRIBAPI_DEF_DIR ~= nil
    then
        break
    end
end

if GRIBAPI_DEF_DIR == nil
then
    GRIBAPI_DEF_DIR = "/usr/share/eccodes/definitions/"
end