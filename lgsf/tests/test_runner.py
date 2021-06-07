import io

from lgsf.runner import CommandRunner


def test_runner_no_commands():
    stdout = io.StringIO()
    CommandRunner("", stdout=stdout)

    assert (
        stdout.getvalue()
        == (
            "          %%%%%%%%%          \n"
            "      %%%%%%%%%%%%%%%%%      \n"
            "    %%%%%%%%%%%%%%%%%%%%%    \n"
            "  %%%%%%%%%%%%%%%%%%%%%%%%%  \n"
            " %%%%%%%%%%%%%%%%%%%%   %%%% \n"
            "%%%%%%%%%%%%%%%%%%%     %%%%%\n"
            "%%%%%%%%%%%%%%%%%     %%%%%%%\n"
            "%%%%%    %%%%%%     %%%%%%%%%\n"
            "%%%%%%     %%     %%%%%%%%%%%\n"
            "%%%%%%%%        %%%%%%%%%%%%%\n"
            " %%%%%%%%%    %%%%%%%%%%%%%% \n"
            "  %%%%%%%%%%%%%%%%%%%%%%%%%  \n"
            "    %%%%%%%%%%%%%%%%%%%%%    \n"
            "      %%%%%%%%%%%%%%%%%      \n"
            "\n"
            "        Democracy "
            "Club                                                                                                                                                                                                                    \n"
            "\n"
            "Local Government Scraper Framework\n"
            "Usage: manage.py [subcommand]\n"
            "\n"
            "Available subcommands:\n"
            "\t * councillors\n"
            "\t * templates\n"
            "\t * metadata"
        )
    )
