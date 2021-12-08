#include "tar.h"
#include "tests.h"
// For Subprocess
#include "arki/exceptions.h"
#include <sys/types.h>
#include <sys/wait.h>


namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    typedef typename SegmentTests<Segment, Data>::Fixture Fixture;

    void register_tests() override;
};

Tests<segment::tar::Segment, GRIBData> test1("arki_segment_tar_grib");
Tests<segment::tar::Segment, BUFRData> test2("arki_segment_tar_bufr");
Tests<segment::tar::Segment, ODIMData> test3("arki_segment_tar_odim");
Tests<segment::tar::Segment, VM2Data>  test4("arki_segment_tar_vm2");
Tests<segment::tar::Segment, NCData>  test5("arki_segment_tar_nc");
Tests<segment::tar::Segment, JPEGData>  test6("arki_segment_tar_jpeg");

struct Subprocess
{
    std::vector<std::string> args;
    std::string executable;
    std::string cwd;
    int stdin_fd = -1;
    int stdout_fd = -1;
    int stderr_fd = -1;
    pid_t pid = -1;
    int returncode = 0;

    Subprocess()
    {
    }

    void run()
    {
        if (args.empty())
            throw std::runtime_error("Subprocess.run called with an empty argument list");
        if (executable.empty())
            executable = args[0];

        // Set argv vector
        std::unique_ptr<char*[]> argv(new char*[args.size() + 1]);
        for (unsigned i = 0; i < args.size(); ++i)
            argv[i] = const_cast<char*>(args[i].c_str());
        argv[args.size()] = nullptr;

        pid = fork();
        if (pid == -1)
            throw_system_error("fork failed");

        if (pid == 0)
        {
            if (stdin_fd != -1)
                if (dup2(stdin_fd, 0) == -1)
                    throw_system_error("dup2 of stdin failed");
            if (stdout_fd != -1)
                if (dup2(stdout_fd, 1) == -1)
                    throw_system_error("dup2 of stdout failed");
            if (stderr_fd != -1)
                if (dup2(stderr_fd, 2) == -1)
                    throw_system_error("dup2 of stderr failed");

            // Child
            if (!cwd.empty())
                if (chdir(cwd.c_str()) == -1)
                    throw_system_error("chdir failed");

            execvpe(executable.c_str(), argv.get(), environ);
            throw_system_error("execve failed");
        } else {
            // Parent
        }
    }

    int wait()
    {
        int status;
        pid_t res = waitpid(pid, &status, 0);
        if (res == -1)
            throw_system_error("waitpid failed");
        returncode = WEXITSTATUS(status);
        return returncode;
    }
};

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

this->add_method("filenames", [](Fixture& f) {
    std::shared_ptr<segment::Checker> checker = f.create();
    sys::File tarout("tar.out", O_RDWR | O_CREAT | O_TRUNC);
    Subprocess proc;
    proc.stdout_fd = tarout;
    proc.args = { "tar", "atf", checker->segment().abspath + ".tar" };
    proc.run();
    tarout.close();
    if (proc.wait() != 0)
        throw std::runtime_error("tar exited with error");
    wassert(actual_file("tar.out").contents_equal({"000000." + f.td.format, "000001." + f.td.format, "000002." + f.td.format}));
    sys::unlink("tar.out");
});

}
}

#include "tests.tcc"
