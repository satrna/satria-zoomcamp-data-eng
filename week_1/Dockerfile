# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Install pandas, sqlalchemy, and psycopg2
RUN pip install pandas sqlalchemy psycopg2-binary

# Copy the current directory contents into the container at /app
COPY . /app

# Run main.py when the container launches
CMD ["python", "ingest_data.py"]
