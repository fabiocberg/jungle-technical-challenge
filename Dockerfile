FROM oven/bun:1.4.3 AS dependencies
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM dependencies AS test
COPY . .
CMD ["bun", "run", "test:all"]

FROM dependencies AS build
COPY . .
RUN bun run typecheck && bun run build

FROM oven/bun:1.4.3 AS production-dependencies
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --production --frozen-lockfile

FROM oven/bun:1.4.3-slim AS runtime
WORKDIR /app
COPY --from=production-dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER bun
EXPOSE 3000
CMD ["bun", "dist/main.js"]
