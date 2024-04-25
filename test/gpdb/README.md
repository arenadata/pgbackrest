## GPDB integration tests

This directory contains automated integration tests for validating the functionality and compatibility of pgBackRest with [Greenplum Database (GPDB)](https://github.com/arenadata/gpdb). These tests are designed primarily to be executed within a GPDB Docker container environment.

**Prerequisites**
To run these integration tests, you will need a Docker environment where you can spin up a GPDB container.
It is preferable to have a conainer build from GPDB repo's [Dockerfile](https://github.com/arenadata/gpdb/blob/adb-6.x/arenadata/readme.md) .

**Launch**
To run the tests, execute the script:
```
bash run_docker.sh <container_id> <desirable log directory>
```
If 3rd argument is missing the log directory is not created. For the container mentioned above you can simply run
```
bash run_docker.sh <image_id>
```

After launching the container the test scripts from `scripts` directory are executed.
