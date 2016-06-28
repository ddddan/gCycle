/* gCycle.js - Client side functionality */

function updateTable(data) {
    var eTBody = document.getElementById('tblResultsBody');
    // Clear out any existing rows
    var eChild;
    while ((eChild = eTBody.firstElementChild) !== null) {
        eTBody.removeChild(eChild);
    }
    // Add a new row for each data item
    var eTR, eTD, d, h, amPm, dur;
    for (var i = 0; i < data.length; i++) {
        eTR = document.createElement('tr');
        // Create cells for relevant data
        // Date
        eTD = document.createElement('td');
        eTD.className = 'result date';
        eTD.innerHTML = data[i].date;
        eTR.appendChild(eTD);
        // Start time
        d = new Date(data[i].startTimeMs);
        h = d.getHours();
        amPm = (h >= 12 ? 'PM' : 'AM');
        h = (h == 0 ? 12 : h % 12);
        eTD = document.createElement('td');
        eTD.className = 'result start';
        eTD.innerHTML = h + ':' + ('0' + d.getMinutes()).slice(-2) + ' ' + amPm;
        eTR.appendChild(eTD);
        // End time
        d = new Date(data[i].endTimeMs);
        h = d.getHours();
        amPm = (h >= 12 ? 'PM' : 'AM');
        h = (h == 0 ? 12 : h % 12);
        eTD = document.createElement('td');
        eTD.className = 'result end';
        eTD.innerHTML = h + ':' + ('0' + d.getMinutes()).slice(-2) + ' ' + amPm;
        eTR.appendChild(eTD);
        // Duration
        eTD = document.createElement('td');
        eTD.className = 'result duration';
        eTD.innerHTML = Math.round(data[i].minutes * 10) / 10;
        eTR.appendChild(eTD);
        //
        eTBody.appendChild(eTR);
    }
}

function getData() {
    // AJAX request to get the data
    var x = new XMLHttpRequest();
    x.open('GET', 'get-data', true);
    x.onreadystatechange = function respond() {
        if (x.readyState === 4 && x.status === 200) {
            var data = JSON.parse(x.responseText);
            updateTable(data);
        }
    };
    x.send();
}

window.onload = function () {
    getData();
};
