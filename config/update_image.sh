docker build -t dataset:latest .

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 762438811603.dkr.ecr.us-east-1.amazonaws.com

docker tag dataset:latest 762438811603.dkr.ecr.us-east-1.amazonaws.com/oli/repo
docker push 762438811603.dkr.ecr.us-east-1.amazonaws.com/oli/repo

