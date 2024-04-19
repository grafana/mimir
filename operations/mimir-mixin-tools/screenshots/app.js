const fs = require('fs');
const path = require('path');
const querystring = require('querystring');
const puppeteer = require('puppeteer');
const spawnSync = require('child_process').spawnSync;

const defaultViewportWidth = 1400;
const defaultViewportHeight = 2400;

// This allow to customize the viewport height for specific dashboards.
const customViewportHeight = {
    'mimir-alertmanager-resources': 1900,
    'mimir-compactor-resources': 1000,
    'mimir-config': 800,
    'mimir-object-store': 1400,
    'mimir-overrides': 800,
    'mimir-overview': 1400,
    'mimir-overview-resources': 1700,
    'mimir-overview-networking': 1100,
    'mimir-reads-networking': 2000,
    'mimir-rollout-progress': 800,
    'mimir-scaling': 800,
    'mimir-slow-queries': 600,
    'mimir-tenants': 1200,
    'mimir-top-tenants': 2000,
    'mimir-writes-networking': 1000,
    'mimir-writes-resources': 1600,
    'mimir-remote-ruler-reads': 1800,
    'mimir-remote-ruler-reads-resources': 1100,
    'mimir-remote-ruler-reads-networking': 1400,
};

// Dashboards for which we're not generating the screenshots because their content
// from a demo env is not much interesting.
const skippedDashboards = [
    'mimir-slow-queries',
    'mimir-top-tenants',
];

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

// List all .json dashboards stored at /input and returns the basename
// and UID of each of them.
function listDashboards() {
    var dashboards = [];

    fs.readdirSync('/input').forEach(file => {
        // Parse the dashboard.
        const raw = fs.readFileSync(path.join('/input', file));
        const data = JSON.parse(raw);
        const name = path.basename(file, '.json');

        // Skip dashboards to skip.
        if (skippedDashboards.includes(name)) {
            return;
        }

        dashboards.push({
            name: name,
            uid: data.uid,
        })
    });

    return dashboards
}

async function takeScreenshot(browser, dashboard) {
    const page = await browser.newPage();
    await page.setViewport({
        width: defaultViewportWidth,
        height: customViewportHeight[dashboard.name] ? customViewportHeight[dashboard.name] : defaultViewportHeight,
    });

    // Build the dashboard url.
    const dashboardURL = "http://mixin-serve-grafana:3000/d/" + dashboard.uid + "?" + querystring.stringify({
        "var-datasource": "Mimir",
        "var-cluster": process.env.CLUSTER,
        "var-namespace": dashboard.name.includes("alertmanager") ? process.env.ALERTMANAGER_NAMESPACE : process.env.MIMIR_NAMESPACE,
        "var-user": process.env.MIMIR_USER,
    })

    // Load the dashboard page.
    await page.goto(dashboardURL);
    await sleep(1000);

    // Send keyshortcut to expand all rows.
    await page.keyboard.type('d', { delay: 50 });
    await page.keyboard.down('Shift');
    await page.keyboard.press('E', { delay: 50 });
    await page.keyboard.up('Shift');

    // Wait until network is idle.
    // (I haven't found a better way to wait until all panels are loaded).
    await page.waitForNetworkIdle({idleTime: 1000, timeout: 15000 });

    // Take screenshot.
    const directoryName = dashboard.name.replace(/^(mimir-)/,'');
    const screenshotPath = "/output/" + directoryName + "/" + dashboard.name + ".png";
    await page.screenshot({path: screenshotPath});
    await page.close();

    // Optimize the png (lossless)
    const res = spawnSync('pngquant', ['--force', '--ext', '.png', '--skip-if-larger', '--speed', '1', '--strip', '--quality', '100', '--verbose', screenshotPath]);

    // We accept status code 99 which is returned when pngquant doesn't optimize
    // a file because the output would be larger than the input.
    if (res.status !== 0 && res.status !== 99) {
        throw new Error(`The pngquant command failed to run: ${res.error}`)
    }
}

async function run() {
    // Ensure required environment variables have been set.
    ["CLUSTER", "MIMIR_NAMESPACE", "ALERTMANAGER_NAMESPACE", "MIMIR_USER"].forEach((name) => {
        if (!process.env[name]) {
            throw new Error(`The ${name} environment variable is missing`)
        }
    })

    const browser = await puppeteer.launch({
        headless: "new",
        args: [
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--disable-setuid-sandbox",
            "--no-sandbox",
        ]
    });

    const dashboards = listDashboards();
    for (const dashboard of dashboards) {
        console.log("Taking screenshot of " + dashboard.name);
        await takeScreenshot(browser, dashboard);
    }

    await browser.close();
}

run()
