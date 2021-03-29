#include "tests.h"
#include "base.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct CommonTestsFixture : public stream::StreamTestsFixture
{
    sys::Tempfile tf;

    CommonTestsFixture()
    {
        set_output(StreamOutput::create(tf, 1000));
    }

    std::string streamed_contents() override
    {
        std::string res;
        tf.lseek(0);

        char buf[4096];
        while (size_t sz = tf.read(buf, 4096))
            res.append(buf, sz);

        return res;
    }
};

class Tests : public stream::StreamTests
{
    using StreamTests::StreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestsFixture);
    }
};

Tests test("arki_stream_concrete_timeout");

void Tests::register_tests() {
StreamTests::register_tests();

add_method("concrete_timeout1", [] {
    stream::BlockingSink sink;
    std::vector<uint8_t> filler(sink.pipe_size());

    auto writer = StreamOutput::create(sink.fd(), 1);

    // This won't block
    writer->send_buffer(filler.data(), filler.size());
    // This times out
    wassert_throws(stream::TimedOut, writer->send_buffer(" ", 1));

    sink.empty_buffer();
    wassert_throws(stream::TimedOut, writer->send_line(filler.data(), filler.size()));

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        wassert_throws(stream::TimedOut, writer->send_file_segment(tf1, 1, 6));
    }

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        wassert_throws(stream::TimedOut, writer->send_from_pipe(tf1));
    }
});

add_method("concrete_timeout_send_buffer", [] {
    stream::BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 100);

    {
        stream::WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_buffer("foobar", 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        //wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        stream::WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_buffer("foobar", 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        //wassert(actual(cb_log) == std::vector<size_t>{3, 3, 6});
    }

    {
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        stream::WriteTest t(*writer, sink, 0);
        wassert(actual(writer->send_buffer(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 6);
        wassert(actual(t.total_written) == sink.pipe_size() + 6);
        //wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 6});
    }

    {
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        stream::WriteTest t(*writer, sink, 1);
        wassert(actual(writer->send_buffer(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 6);
        wassert(actual(t.total_written) == sink.pipe_size() + 6);
        //wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size() - 1, 7});
    }
});

add_method("concrete_timeout_send_line", [] {
    stream::BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    {
        stream::WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        wassert(actual(t.cb_log) == std::vector<size_t>{0, 7});
    }

    {
        stream::WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7});
    }

    {
        stream::WriteTest t(*writer, sink, -7);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        stream::WriteTest t(*writer, sink, 0);
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        wassert(actual(writer->send_line(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 7);
        wassert(actual(t.total_written) == sink.pipe_size() + 7);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        stream::WriteTest t(*writer, sink, 1);
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        wassert(actual(writer->send_line(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 7);
        wassert(actual(t.total_written) == sink.pipe_size() + 7);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }
});

add_method("concrete_timeout_send_file_segment", [] {
    stream::BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("foobarbaz"));
    tf1.lseek(sink.pipe_size());
    tf1.write_all_or_throw(std::string("zabraboof"));

    {
        stream::WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        stream::WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        wassert(actual(t.cb_log) == std::vector<size_t>{6});
    }

    {
        stream::WriteTest t(*writer, sink, -7);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        stream::WriteTest t(*writer, sink, 0);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }

    {
        stream::WriteTest t(*writer, sink, 1);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }
});

add_method("concrete_timeout_send_from_pipe", [] {
    stream::BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("foobarbaz"));
    tf1.lseek(sink.pipe_size());
    tf1.write_all_or_throw(std::string("zabraboof"));

    {
        stream::WriteTest t(*writer, sink, -3);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == 9u);
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        stream::WriteTest t(*writer, sink, -9);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == 9u);
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(t.cb_log) == std::vector<size_t>{9});
    }

    {
        stream::WriteTest t(*writer, sink, -10);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == 9u);
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(t.cb_log) == std::vector<size_t>{9});
    }

    {
        stream::WriteTest t(*writer, sink, 0);
        tf1.lseek(0);
        wassert(actual(writer->send_from_pipe(tf1)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }

    {
        stream::WriteTest t(*writer, sink, 1);
        tf1.lseek(0);
        wassert(actual(writer->send_from_pipe(tf1)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }
});


}

}
