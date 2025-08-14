# AWS EB - Streamlit webapp
Running a docker image of the Streamlit web application using AWS Elastic Beanstalk.

### Creating and pushing Docker image 

1. Creating local docker image of the app.
    ```bash
    docker build -t streamlit-app .
    ```
2. Tagging the docker image for the ECR repository
    ```bash
    docker tag streamlit-app:latest 008405939996.dkr.ecr.eu-west-1.amazonaws.com/streamlit-app
    ```
3. Push image to AWS ECR (Elastic Container Registry)
    ```bash
    docker push 008405939996.dkr.ecr.eu-west-1.amazonaws.com/streamlit-app
    ```

> **OPTIONAL:**
> 
> Make sure you are logged in to ECR:
> ```bash
> aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 008405939996.dkr.ecr.eu-west-1.amazonaws.com
> ```

### Running application with Beanstalk

4. Creating/Initializing beanstalk application:
    ```bash
    eb init -p docker streamlit-app
    ```
5. This will create the file```.elasticbeanstalk/config.yml```, where we can configure the enviroment. In our case, we only change the region field:
    ```yml
    default_region: eu-west-1
    ```
6. Next, since we want AWS Beanstalk to use a Docker image from AWS ECR, we create the file ```Dockerrun.aws.json``` in the app root directory. 
    ```json
    {
      "AWSEBDockerrunVersion": "1",
      "Image": {
        "Name": "<ECR-Repository-URI>",
        "Update": "true"
      },
      "Ports": [
        {
          "ContainerPort": "8501"
        }
      ]
    }
    ```
7. Create enviroment to run the image in a container:
    ```bash
    eb create streamlit-app
    ```
    This will create an Elastic Beanstalk enviroment (EC2 instances, Load Balancer, etc) and deploy the application using the Docker image from ECR.

> **OPTIONAL:** 
>
> Open application in browser:
> ```bash
> eb open
> ```
> Re-launch the application, fetching the Docker image again:
> ```bash
> eb deploy
> ```
