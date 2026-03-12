FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY step_1.py ${LAMBDA_TASK_ROOT}
COPY step_2.py ${LAMBDA_TASK_ROOT}
COPY step_3.py ${LAMBDA_TASK_ROOT}

