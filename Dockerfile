FROM oven/bun:1-alpine

WORKDIR /app

COPY package.json ./
RUN bun install --production

COPY . .

ENV PORT=8080

CMD ["bun", "run", "src/index.ts"]
