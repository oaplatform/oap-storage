#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) Open Application Platform Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

set -x

BUILD_COUNTER=$1
PROJECT_NAME=$2
BRANCH_NAME=$3

VERSION_XENOSS=$(grep -oP 'project\.version\>\K[^<]*' pom.xml)

if [ "$BRANCH_NAME" == "master" ] || [ "$BRANCH_NAME" == "" ] || [ "$BRANCH_NAME" == "refs/heads/master" ]; then
  VERSION_BRANCH=""
  MAVEN_BUILD_COUNTER=""
else
  VERSION_BRANCH="-${BRANCH_NAME}"
  MAVEN_BUILD_COUNTER="-${BUILD_COUNTER}"
fi

set +x

#project name
echo "##teamcity[setParameter name='oap.project.name' value='${PROJECT_NAME,,}']"

#maven master
echo "##teamcity[setParameter name='oap.storage.project.version' value='${VERSION_XENOSS}']"

#maven branch
echo "##teamcity[setParameter name='oap.project.version.branch' value='${VERSION_XENOSS}${VERSION_BRANCH}${MAVEN_BUILD_COUNTER}']"

#teamcity
set +x

echo "##teamcity[buildNumber '${VERSION_XENOSS}${VERSION_BRANCH}-${BUILD_COUNTER}']"
