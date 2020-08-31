#https://devcenter.heroku.com/articles/container-registry-and-runtime

# log in to heroku
heroku login

# create dyno/instance/whatever
heroku create

# change stack to container for docker deploy
heroku stack:set container

# Execute the following for re-deploy:
# Make sure Docker is running locally

# log in to container registry
heroku container:login

# build and deploy docker image to heroku registry
# "worker" is the process type. Still not 100% clear on what options are avaiable or what they mean contextually
heroku container:push worker

# create a "release"
heroku container:release worker

# check the logs
# https://devcenter.heroku.com/articles/logging
heroku logs
heroku logs -n 200
heroku logs --tail

# list apps
heroku apps

# restart dyno
heroku dyno:restart -a powerful-tundra-96461

 #>