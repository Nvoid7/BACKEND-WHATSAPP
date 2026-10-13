FROM node:20-alpine
WORKDIR /app
RUN apk add --no-cache git
COPY package.json ./
RUN npm install --omit=dev
COPY server.js ./
ENV DATA_DIR=/data
VOLUME /data
EXPOSE 3100
CMD ["node","server.js"]
