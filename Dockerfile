# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# syntax=docker/dockerfile:1

############################# BUILD ###############################
# This stage builds the HarbourBridge binary using its source code.

# Add golang as the base image for the build stage.
FROM golang:1.18.3 AS build

# Set the default work directory as “/hbBin” in the container.
WORKDIR /harbourbridge_bin

# Copy HarbourBridge’s source code into the working directory.
COPY . ./
# Download HarbourBridge’s dependencies mentioned in the go.mod file.
RUN go mod tidy 
# Compile the source code and store the binary in the work directory /hbBin.
RUN make build-static  

########################### RELEASE ###############################
# This stage consists of the GCloud SDK and the HarbourBridge binary.

# Add the Ubuntu base image from the Google Container Registry.
FROM gcr.io/cloud-marketplace/google/ubuntu1804@sha256:d9fe979ab72ff02f5b5d95c59cda6681aa73039fbf9b46596935267cf26c692e
# Set the default working directory.
# This directory binds with the host directory that is to store the output files generated by HarbourBridge.
WORKDIR /harbourbridge/harbour_bridge_output
# Create directory to store the HarbourBridge binary. 
RUN mkdir /harbourbridge/bin
# Create directory to store the dump files. This directory binds with the host directory that contains the dump files of the source database.
RUN mkdir /harbourbridge/sourceDump
# Copy the HarbourBridge binary from the build stage's hbBin directory to the release stage's harbourbridge/bin directory.
COPY --from=build /harbourbridge_bin/harbourbridge /harbourbridge/bin

# Add Python3 to support GCloud SDK.
RUN apt-get update
RUN apt-get install -y python3.10

# Download the gcloud package.
RUN curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-391.0.0-linux-x86_64.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Install the GCloud package.
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Add gcloud to $PATH.
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

