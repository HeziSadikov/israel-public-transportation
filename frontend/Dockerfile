FROM node:20-bookworm-slim

WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend .

EXPOSE 5173

# Avoid `npm run dev` as container PID 1 — npm swallows signals poorly; SIGTERM then surfaces as npm "command failed".
CMD ["node", "node_modules/vite/bin/vite.js", "--host", "0.0.0.0"]
