const API_BASE = "http://localhost:8000";

async function loadScores() {

    const response = await fetch(
        `${API_BASE}/scores`
    );

    const data = await response.json();

    let html = "";

    for (const match in data) {

        const item = data[match];

        html += `
            <div class="item">
                <strong>${item.teams[0]}</strong>
                ${item.score[0]}
                -
                ${item.score[1]}
                <strong>${item.teams[1]}</strong>
            </div>
        `;
    }

    document.getElementById("scores").innerHTML = html;
}


async function loadAlerts() {

    const response = await fetch(
        `${API_BASE}/alerts`
    );

    const data = await response.json();

    let html = "";

    data.forEach(alert => {

        html += `
            <div class="item">
                ${alert.eventType}
                -
                ${alert.matchId}
            </div>
        `;
    });

    document.getElementById("alerts").innerHTML = html;
}


async function loadLag() {

    const response = await fetch(
        `${API_BASE}/lag`
    );

    const data = await response.json();

    let html = "";

    for (const topic in data) {

        html += `<h4>${topic}</h4>`;

        for (const partition in data[topic]) {

            html += `
                <div class="item">
                    Partition ${partition} :
                    ${data[topic][partition]}
                </div>
            `;
        }
    }

    document.getElementById("lag").innerHTML = html;
}


async function refreshDashboard() {

    await loadScores();

    await loadAlerts();

    await loadLag();
}


refreshDashboard();

setInterval(
    refreshDashboard,
    5000
);