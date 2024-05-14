# Use an official AWS Glue 4 image as a base image
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Install pytest and any other Python dependencies
RUN pip install pytest

# Add your project files to the Docker image
ADD . /app
WORKDIR /app

# Define the entrypoint to run pytest
ENTRYPOINT ["pytest"]