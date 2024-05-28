# Use the official Python image as a base
FROM python:3.8-slim


RUN pip install psycopg2-binary


# Set environment variables
ENV AIRFLOW_HOME=/airflow
ENV AIRFLOW_USER_HOME=/airflow
ENV AIRFLOW_UID: 50000

# Install dependencies
RUN pip install apache-airflow

# Initialize Airflow database
RUN airflow db init

# Expose the Airflow web server port
EXPOSE 8080

# Start Airflow web server
CMD ["airflow", "webserver", "--port", "8080"]


