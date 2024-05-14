ARG GLUE_VERSION="4.0.0"
ARG USER=glue_user

FROM amazon/aws-glue-libs:glue_libs_${GLUE_VERSION}_image_01

USER root
RUN usermod -aG wheel glue_user

USER $USER
# Poetry installation
ARG HOME="/home/$USER"
ARG POETRY_VERSION="none"

ENV PATH="${HOME}/.local/bin:$PATH"

RUN if [ "${POETRY_VERSION}" != "none" ]; then  echo "Installing poetry" \
    && curl -sSL https://install.python-poetry.org | POETRY_VERSION=${POETRY_VERSION} python3 - ; \
    fi


# Add your project files to the Docker image
ADD . /app
WORKDIR /app

# Install pytest and any other Python dependencies
RUN poetry install

# Define the entrypoint to run pytest
ENTRYPOINT ["pytest"]