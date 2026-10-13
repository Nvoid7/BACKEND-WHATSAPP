FROM node:20-bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates openssh-client \
 && rm -rf /var/lib/apt/lists/* \
 && git config --global url."https://github.com/".insteadOf ssh://git@github.com/ \
 && git config --global url."https://github.com/".insteadOf git@github.com: \
 && git config --global url."https://".insteadOf git://
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY server.js ./
ENV DATA_DIR=/data
EXPOSE 3100
CMD ["node","server.js"]

