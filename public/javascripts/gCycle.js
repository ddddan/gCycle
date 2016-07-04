/* gCycle.js - Client side functionality */

var scriptData = {};

function addCell(eTR, value, className) {
    eTD = document.createElement('td');
    eTD.className = className;
    eTD.innerHTML = value;
    eTR.appendChild(eTD);
    return eTD;
}

function addConfidenceGraph(eTD, confArray) {
    // Constants - will be updated later
    var C = {};
    C.canvasWidth = 400;
    C.canvasHeight = 60;
    C.marginX = C.marginY = 10;
    C.top = C.marginY;
    C.bottom = C.canvasHeight - C.marginY;
    C.leftBound = C.marginX;
    C.rightBound = C.canvasWidth - C.marginX;
    C.innerWidth = C.rightBound - C.leftBound;
    C.colorMap = [
        '#DAFFFF',
        '#C1FEFF',
        '#A8E5FF',
        '#8ECBFF',
        '#75B2E7',
        '#5B98CD',
        '#427FB4',
        '#28659A',
        '#0F4C81',
        '#003267'
    ];

    var cvs = document.createElement('canvas');
    cvs.setAttribute('width', C.canvasWidth);
    cvs.setAttribute('height', C.canvasHeight);

    eTD.appendChild(cvs);

    var ctx = cvs.getContext('2d');

    ctx.strokeRect(C.leftBound, C.top, C.innerWidth, (C.bottom - C.top));

    //var sliceWidth = (C.rightBound - C.leftBound) / confArray.length;
    for (var i = 0; i < confArray.length - 1; i++) {
        // var leftBound = i * sliceWidth + C.leftBound;
        var leftBound = ((confArray[i].timestampMs - confArray[0].timestampMs) / (confArray[confArray.length - 1].timestampMs - confArray[0].timestampMs)) * C.innerWidth + C.leftBound;
        var sliceWidth = ((confArray[i + 1].timestampMs - confArray[i].timestampMs) / (confArray[confArray.length - 1].timestampMs - confArray[0].timestampMs)) * C.innerWidth;
        ctx.fillStyle = C.colorMap[Math.round(confArray[i].confidence / C.colorMap.length)];
        ctx.fillRect(leftBound, C.top, sliceWidth, C.bottom - C.top);
    }


    // eTD.appendChild(cvs);
}



function updateTable(data) {
    var eTBody = document.getElementById('tblResultsBody');
    // Clear out any existing rows
    var eChild;
    while ((eChild = eTBody.firstElementChild) !== null) {
        eTBody.removeChild(eChild);
    }
    // Add a new row for each data item
    var eTR, eTD, d, h, h12, amPm, dur;
    for (var i = 0; i < data.length; i++) {
        eTR = document.createElement('tr');
        // Create cells for relevant data
        // Date
        addCell(eTR, data[i].date, 'result date');
        // Start time
        d = new Date(data[i].startTimeMs);
        // Convert to 12 hour time
        h = d.getHours();
        amPm = (h >= 12 ? 'PM' : 'AM');
        h12 = ((h + 11) % 12) + 1;
        addCell(eTR, (h12 + ':' + ('0' + d.getMinutes()).slice(-2) + ' ' + amPm), 'result start');
        // End time
        d = new Date(data[i].endTimeMs);
        // Convert to 12 hour time
        h = d.getHours();
        amPm = (h >= 12 ? 'PM' : 'AM');
        h12 = ((h + 11) % 12) + 1;
        addCell(eTR, (h12 + ':' + ('0' + d.getMinutes()).slice(-2) + ' ' + amPm), 'result end');
        // Duration
        addCell(eTR, Math.round(data[i].minutes * 10) / 10, 'result duration');
        // Confidence
        var confCell = addCell(eTR, '', 'result confidence');
        addConfidenceGraph(confCell, data[i].points);
        eTBody.appendChild(eTR);
    }
}

function getData(forceReload) {
    if (scriptData.requestSent === true) {
        // Try to prevent multiple requests
        return;
    }
    // AJAX request to get the data
    var x = new XMLHttpRequest();
    var params = '';
    if (forceReload === true) {
        params = '?noReload=0';
    }
    x.open('GET', 'get-data' + params, true);
    x.onreadystatechange = function respond() {
        if (x.readyState === 4 && x.status === 200) {
            var data = JSON.parse(x.responseText);
            updateTable(data);
            scriptData.requestSent = false;
        }
    };
    x.send();
    scriptData.requestSent = true;
}

function forceReload() {
    getData(true);
}

window.onload = function () {
    scriptData.requestSent = false;
    getData();
    document.getElementById('refresh').addEventListener('click', forceReload);
};
