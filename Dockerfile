# Start from the Airflow image
FROM apache/airflow:2.5.1

# Change to the root user to install packages
USER root

# Copy your requirements file into the Docker container
COPY requirements.txt /requirements.txt

# Set environment variable for additional requirements
ARG _PIP_ADDITIONAL_REQUIREMENTS

# Install the Python packages
RUN pip install -r /requirements.txt
RUN if [ -n "$_PIP_ADDITIONAL_REQUIREMENTS" ]; then pip install $_PIP_ADDITIONAL_REQUIREMENTS; fi

# Change back to the airflow user
USER airflow