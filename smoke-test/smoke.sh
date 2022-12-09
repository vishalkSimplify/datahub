#!/bin/bash
set -euxo pipefail

# Runs a basic e2e test. It is not meant to be fully comprehensive,
# but rather should catch obvious bugs before they make it into prod.
#
# Script assumptions:
#   - The gradle build has already been run.
#   - Python 3.6+ is installed and in the PATH.

# Log the locally loaded images
# docker images | grep "datahub-"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

echo "DATAHUB_VERSION = $DATAHUB_VERSION"
DATAHUB_TELEMETRY_ENABLED=false  \
DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
datahub docker quickstart --standalone_consumers --dump-logs-on-failure

(cd ..; ./gradlew :smoke-test:yarnInstall)

export CYPRESS_ADMIN_USERNAME=${ADMIN_USERNAME:-datahub}
export CYPRESS_ADMIN_PASSWORD=${ADMIN_PASSWORD:-datahub}

pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke.xml
