# Start from the AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.9

# Install necessary dependencies
RUN yum update -y \
    && yum install -y \
        gcc \
        g++ \
        python3-dev \
    && yum clean all \
    && pip install --upgrade pip

# Set the working directory to /var/task
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy your files into the Docker container
COPY requirements.txt ${LAMBDA_TASK_ROOT}
COPY .env ${LAMBDA_TASK_ROOT}
COPY async_crawler.py ${LAMBDA_TASK_ROOT}
COPY problem.py ${LAMBDA_TASK_ROOT}

# Install the Python packages
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could be a different file and function)
CMD ["problem.lambda_handler"]