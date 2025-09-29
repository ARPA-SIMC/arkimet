#include "arki/core/cfg.h"
#include "arki/dataset/file.h"
#include "arki/dataset/session.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/metadata/tests.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_file");

void Tests::register_tests()
{

    add_method("grib", [] {
        auto cfg = dataset::file::read_config("inbound/test.grib1");
        wassert(actual(cfg->value("name")) == "inbound/test.grib1");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "grib");
    });

    add_method("grib_as_bufr", [] {
        auto cfg = dataset::file::read_config("bUFr", "inbound/test.grib1");
        wassert(actual(cfg->value("name")) == "inbound/test.grib1");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "bufr");
    });

    add_method("grib_strangename", [] {
        system("cp inbound/test.grib1 strangename");
        auto cfg = dataset::file::read_config("GRIB", "strangename");
        wassert(actual(cfg->value("name")) == "strangename");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "grib");

        // Scan it to be sure it can be read
        auto session = std::make_shared<dataset::Session>();
        metadata::Collection mdc(*session->dataset(*cfg), Matcher());
        wassert(actual(mdc.size()) == 3u);
    });

    add_method("metadata", [] {
        auto cfg = dataset::file::read_config("inbound/odim1.arkimet");
        wassert(actual(cfg->value("name")) == "inbound/odim1.arkimet");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "arkimet");

        // Scan it to be sure it can be read
        auto session = std::make_shared<dataset::Session>();
        metadata::Collection mdc(*session->dataset(*cfg), Matcher());
        wassert(actual(mdc.size()) == 1u);
    });

    add_method("metadata_read_data", [] {
        {
            std::filesystem::remove("test.grib1");
            std::filesystem::create_hard_link("inbound/test.grib1",
                                              "test.grib1");
            metadata::TestCollection initial("test.grib1");
            initial.writeAtomically("test.metadata");
        }

        auto cfg = dataset::file::read_config("test.metadata");
        wassert(actual(cfg->value("name")) == "test.metadata");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "arkimet");

        // Scan it to be sure it can be read
        auto session = std::make_shared<dataset::Session>();
        metadata::Collection mdc(*session->dataset(*cfg),
                                 query::Data(Matcher(), true));
        wassert(actual(mdc.size()) == 3u);

        auto cwd = std::filesystem::current_path();
        wassert(
            actual_type(mdc[0].sourceBlob())
                .is_source_blob(DataFormat::GRIB, cwd, "test.grib1", 0, 7218));
        wassert(actual_type(mdc[1].sourceBlob())
                    .is_source_blob(DataFormat::GRIB, cwd, "test.grib1", 7218,
                                    34960));
        wassert(actual_type(mdc[2].sourceBlob())
                    .is_source_blob(DataFormat::GRIB, cwd, "test.grib1", 42178,
                                    2234));

        wassert(actual(mdc[0].get_data().size()) == 7218u);
        wassert(actual(mdc[1].get_data().size()) == 34960u);
        wassert(actual(mdc[2].get_data().size()) == 2234u);

        wassert_true(mdc[0].sourceBlob().reader == mdc[1].sourceBlob().reader);
        wassert_true(mdc[0].sourceBlob().reader == mdc[2].sourceBlob().reader);
    });

    add_method("yaml", [] {
        auto cfg = dataset::file::read_config("inbound/issue107.yaml");
        wassert(actual(cfg->value("name")) == "inbound/issue107.yaml");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "yaml");

        // Scan it to be sure it can be read
        auto session = std::make_shared<dataset::Session>();
        metadata::Collection mdc(*session->dataset(*cfg), Matcher());
        wassert(actual(mdc.size()) == 1u);
    });

    add_method("absolute_path", [] {
        // Enter into a directory which is not the parent of where the file is
        std::filesystem::create_directory("test");
        files::Chdir chdir("test");
        auto cfg = dataset::file::read_config("../inbound/test.grib1");
        wassert(actual(cfg->value("name")) == "../inbound/test.grib1");
        wassert(actual(cfg->value("type")) == "file");
        wassert(actual(cfg->value("format")) == "grib");

        // Scan it to be sure it can be read
        auto session = std::make_shared<dataset::Session>();
        metadata::Collection mdc(*session->dataset(*cfg), Matcher());
        wassert(actual(mdc.size()) == 3u);
    });
}

} // namespace
