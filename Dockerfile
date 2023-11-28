# Use a Python base image
FROM python:3.11.2

# Set the working directory in the container
WORKDIR /alarstudios_test

# Copy the Pipfile and Pipfile.lock
COPY Pipfile Pipfile.lock ./

# Install Pipenv and dependencies
RUN pip install pipenv && pipenv install --system --deploy

# Copy the Flask application code
COPY . .

# Expose the port on which the Flask application will run
EXPOSE 5000

# Run the Flask application
CMD ["pipenv", "run", "python", "app.py"]