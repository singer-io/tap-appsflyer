FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Authorize SSH Host
RUN mkdir -p ~/.ssh && \
    chmod 0700 ~/.ssh && \
    ssh-keyscan github.com > ~/.ssh/known_hosts

RUN pip3 install --upgrade pip wheel setuptools build