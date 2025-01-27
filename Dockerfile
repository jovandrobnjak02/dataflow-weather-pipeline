FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

RUN apt-get update && apt-get install -y libffi-dev && rm -rf /var/lib/apt/lists/*

COPY . .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"

RUN python3 -m pip install apache-beam[gcp]==2.58.1

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]