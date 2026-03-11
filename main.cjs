const { spawn } = require("child_process");
const { app, BrowserWindow } = require("electron");
const path = require("path");
const http = require("http");
const fs = require("fs");
const url = require("url");

function startServer(port = 8000) {
  const root = __dirname;

  const mimeTypes = {
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
  };

  const server = http.createServer((req, res) => {
    const parsedUrl = url.parse(req.url);
    let pathname = path.join(root, decodeURIComponent(parsedUrl.pathname));

    if (parsedUrl.pathname === "/" || parsedUrl.pathname === "/viewer/") {
      pathname = path.join(root, "viewer", "index.html");
    }

    fs.stat(pathname, (err, stats) => {
      if (err) {
        res.statusCode = 404;
        res.end("Not found");
        return;
      }

      let filePath = pathname;
      if (stats.isDirectory()) {
        filePath = path.join(pathname, "index.html");
      }

      const ext = path.extname(filePath).toLowerCase();
      const type = mimeTypes[ext] || "application/octet-stream";

      fs.readFile(filePath, (readErr, data) => {
        if (readErr) {
          res.statusCode = 500;
          res.end("Server error");
          return;
        }

        res.setHeader("Content-Type", type);
        res.end(data);
      });
    });
  });

  return new Promise((resolve) => {
    server.listen(port, () => resolve(server));
  });
}

function startEngine() {
  const engine = spawn("node", ["index.js"], {
    cwd: __dirname,
    stdio: "ignore",
    detached: true
  });

  engine.unref();
}

async function createWindow() {
  await startServer(8000);

  const win = new BrowserWindow({
    width: 1440,
    height: 980,
    backgroundColor: "#0b1020",
    autoHideMenuBar: true,
    webPreferences: {
      contextIsolation: true,
    },
  });

  win.loadURL("http://localhost:8000/viewer/");
}

app.whenReady().then(() => {
  // startEngine();
  createWindow();
});


app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
