DOCKER=docker
IMGTAG=alexdarancio7/stelar_image2ts:latest
IMGPATH=.
DOCKERFILE=$(IMGPATH)/Dockerfile

.PHONY: all build push

all: build push

build:
	$(DOCKER) build -f $(DOCKERFILE) $(IMGPATH) -t $(IMGTAG)

push:
	$(DOCKER) push $(IMGTAG)

