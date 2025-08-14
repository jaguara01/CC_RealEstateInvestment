# AWS EB - Streamlit webapp
Running a docker image of the Streamlit web application using AWS Elastic Beanstalk.

### Creating and pushing Docker image 

1. Creating local docker image of the app.
    ```bash
    docker build -t inmodecision .
    ```
2. Tagging the docker image for the ECR repository
    ```bash
    docker tag inmodecision:latest <user-id>.dkr.ecr.eu-west-1.amazonaws.com/<repository-name>
    ```
3. Push image to AWS ECR (Elastic Container Registry)
    ```bash
    docker push <user-id>.dkr.ecr.eu-west-1.amazonaws.com/<repository-name>
    ```

> **OPTIONAL:**
> 
> Make sure you are logged in to ECR:
> ```bash
> aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin <user-id>.dkr.ecr.eu-west-1.amazonaws.com
> ```

### Running application with Beanstalk

4. Creating/Initializing beanstalk application:
    ```bash
    eb init -p docker inmodecision
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
    eb create inmodecision-prod \
        --platform "docker" \
        --instance_type t3.small \
        --envvars AWS_ACCESS_KEY_ID=<access-key>,AWS_SECRET_ACCESS_KEY=<secret-access-key>,AWS_REGION=eu-west-1,S3_BUCKET=stream-worker-files
    ```
    This will create an Elastic Beanstalk enviroment (EC2 instances, Load Balancer, etc) and deploy the application using the Docker image from ECR. We specify as instance type t3.small, since t3.nano or t3.micro do not have enough resources (RAM) to run the Streamlit app without crashing. We also create the necessary enviroment variables that the application will need.

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

