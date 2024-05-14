ARG GLUE_VERSION="4.0.0"
ARG USER=glue_user

FROM amazon/aws-glue-libs:glue_libs_${GLUE_VERSION}_image_01

USER root
RUN usermod -aG wheel glue_user

USER $USER
ARG HOME="/home/$USER"
ARG POETRY_VERSION="none"
ENV PATH="${HOME}/.local/bin:$PATH"

ADD . /app
WORKDIR /app

RUN if [ "${POETRY_VERSION}" != "none" ]; then \
    && curl -sSL https://install.python-poetry.org | POETRY_VERSION=${POETRY_VERSION} python3 - ; \
    fi

RUN poetry install

ENTRYPOINT ["pytest"]