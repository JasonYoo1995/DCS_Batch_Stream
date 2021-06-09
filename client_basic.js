var Gearman = require("abraxas");

var batchMode = false;
var streamMode = true;

var studentNum = 31;
var problemNum = 25; // must be equal or less than 25
var nodeNum = 4;

var serverList = []; // List of Connected Servers
var portList = [];

var score = [];

var taskNum = 0;

var AVmode = 0; // 0 = FT Reconnection, 1 = FT Reassignment, 2 = Voting

var sumInput = [];
var countInput = [];
var countTemp = [];

var sumOutputNum = 0; // batch mode

var sumRRpointer = 0;
var countRRpointer = 0;

var finish;
var output = [];

// sleep
function timeout(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function setMode() {
    var arg = Number(process.argv[2]);
    if (arg == 0) batchMode = true;
    else if (arg == 1) streamMode = true;
    AVmode = Number(process.argv[3]);
    if (AVmode == 2)
        batchMode = true;
}

function connectServers() {
    for (let i = 0; i < nodeNum; i++) {
        let client = Gearman.Client.connect({
            servers: ["127.0.0.1:" + (i + 4730)],
            defaultEncoding: "utf8",
        });
        client.on('connect', function (client) {
            serverList.push(client); // Add to ServerList
            portList.push(i);
        });
    }
}

function setValues() {
    var quotient;
    var remainder;
    var inputPerBatch = 7; // for stream mode

    // initialize job
    for (let i = 0; i < studentNum; i++) {
        var temp = [];
        for (let j = 0; j < problemNum; j++) {
            temp.push([i, getRandomScore()]); // [student ID, score]
        }
        score.push(temp);
    }

    // divide job into task, and then push task into sum queue
    if (AVmode == 2) {
        taskNum = nodeNum;
        sumInput.push(score);
        sumInput.push(score);
        sumInput.push(score);
        sumInput.push(score);
    }
    else if (batchMode) {
        // studentNum   taskNum = nodeNum  quotient    (quotient+remainder)
        //     29               3            9 9               (9+2)
        //     30               3           10 10              (10+0)
        //     31               3           10 10              (10+1)
        quotient = Math.floor(studentNum / nodeNum); // 10
        remainder = studentNum % nodeNum;
        taskNum = nodeNum;

        for (var i = 0; i < taskNum; i++) {
            var input = [];
            for (var j = 0; j < quotient; j++) {
                input.push(score[i * quotient + j]);
            }
            // last loop
            if (i == taskNum - 1) {
                for (var j = 0; j < remainder; j++) {
                    input.push(score[quotient * nodeNum + j]);
                }
            }
            sumInput.push(input);
        }
    } else if (streamMode) {
        // studentNum  IPB    quotient (remainder)   tasknum
        //     27       7     7 7 7        (6)          4
        //     28       7     7 7 7 7                   4
        //     29       7     7 7 7 7      (1)          5
        quotient = Math.floor(studentNum / inputPerBatch);
        remainder = studentNum % inputPerBatch;
        taskNum = Math.floor((studentNum - 1) / inputPerBatch) + 1;

        for (var i = 0; i <= quotient; i++) {
            var input = [];
            for (var j = 0; j < inputPerBatch; j++) {
                if (i == quotient && j == remainder) break;
                input.push(score[i * inputPerBatch + j]);
            }
            if (input.length > 0) sumInput.push(input);
        }
    }

    console.log(score);
    console.log("----------------------------------------");
    for (var i = 0; i < sumInput.length; i++) console.log(sumInput[i]);
}

function getRandomScore() {
    return Math.floor(Math.random() * 5);
}

function initializeOutput() {
    for (var i = 0; i <= 100; i++) {
        output.push([i, 0]);
    }
}

function executeSum() {
    if (sumInput.length == 0) return;

    let client = serverList[sumRRpointer];
    sumRRpointer++;
    sumRRpointer %= serverList.length; // control index

    // execute
    let input = sumInput[0];
    sumInput.splice(0, 1); // remove item on index 0
    let startTime = Date.now();

    let disconnectFunction = function (client) {
        let portNum = portList[serverList.indexOf(client)];
        if (AVmode == 1) { // Reassignment
            if (serverList.indexOf(client) >= 0 && serverList.indexOf(client) < serverList.length) {
                console.log('[ERROR] Fail From Server ' + portNum);
                serverList.splice(serverList.indexOf(client), 1); // Remove From List
                portList.splice(serverList.indexOf(client), 1);
                client.disconnect();
                console.log('Will Remove From Server List');
                sumRRpointer %= serverList.length;
            }
            sumInput.push(input);
        } else if (AVmode == 0) { // Reconnection
            console.log('[ERROR] Fail From Server ' + portNum);
            console.log('Will try to Reconnect');
            console.log(input.length);
            client.on('connect', function (client) {
                console.log('Reconnected to Server ' + portNum);
                sumRRpointer = serverList.indexOf(client);
                sumInput.push(input);
            });
        } else if (AVmode == 2) { // Voting
            if (serverList.indexOf(client) >= 0 && serverList.indexOf(client) < serverList.length) {
                console.log('[ERROR] Fail From Server ' + portNum);
                serverList.splice(serverList.indexOf(client), 1); // Remove From List
                portList.splice(serverList.indexOf(client), 1);
                client.disconnect();
                console.log('Will Remove From Server List');
            }
        }
    }

    client
        .submitJob("sum", JSON.stringify([input, startTime]))
        .then(function (result) {
            client.removeListener('disconnect', disconnectFunction);
            let [sum, startTime] = JSON.parse(result);
            let endTime = Date.now();
            let time = endTime - startTime;
            console.log(`------------SUM : ${time}ms elapsed------------`);
            countInput.push(sum);
            console.log(sum);
            if (AVmode == 2 && (countInput.length == serverList.length)) {
                let sumOut = [];
                let tmp;
                for (let i = 0; i < countInput[0].length; i++) { // i번째 학생
                    let scoreCount = new Map();
                    let studentId = countInput[0][i][0];
                    for (let j = 0; j < countInput.length; j++) { // j 번째 input count
                        if (scoreCount.has(countInput[j][i][1])) {
                            tmp = scoreCount.get(countInput[j][i][1]);
                            scoreCount.delete(countInput[j][i][1]);
                            scoreCount.set(countInput[j][i][1], tmp + 1);
                        } else
                            scoreCount.set(countInput[j][i][1], 1);
                    }
                    scoreCount = [...scoreCount.entries()].sort();
                    sumOut.push([studentId, scoreCount[scoreCount.length - 1][0]]);
                }
                countInput = [];
                for (let i = 0; i < serverList.length; i++)
                    countInput.push(sumOut);
                console.log('success');
            }
            sumOutputNum++; // for batch mode
        });
    client.on('disconnect', disconnectFunction);
}

function executeCount() {
    if (batchMode && sumOutputNum < taskNum) return;
    if (countInput.length == 0) return;

    client = serverList[countRRpointer];

    countRRpointer++;
    countRRpointer %= serverList.length;

    // execute
    let input = countInput[0];
    countInput.splice(0, 1); // remove item on index 0
    let startTime = Date.now();

    let disconnectFunction = function (client) {
        let portNum = portList[serverList.indexOf(client)];
        if (AVmode == 1) { // Reassignment
            if (serverList.indexOf(client) >= 0 && serverList.indexOf(client) < serverList.length) {
                console.log('[ERROR] Fail From Server ' + portNum);
                serverList.splice(serverList.indexOf(client), 1); // Remove From List
                portList.splice(serverList.indexOf(client), 1);
                client.disconnect();
                console.log('Will Remove From Server List');
                countRRpointer %= serverList.length;
            }
            countInput.push(input);
        } else if (AVmode == 0) { // Reconnection
            console.log('[ERROR] Fail From Server ' + portNum);
            console.log('Will try to Reconnect');
            console.log(input.length);
            client.on('connect', function (client) {
                console.log('Reconnected to Server ' + portNum);
                countRRpointer = serverList.indexOf(client);
                countInput.push(input);
            });
        } else if (AVmode == 2) { // Voting
            if (serverList.indexOf(client) >= 0 && serverList.indexOf(client) < serverList.length) {
                console.log('[ERROR] Fail From Server ' + portNum);
                serverList.splice(serverList.indexOf(client), 1); // Remove From List
                portList.splice(serverList.indexOf(client), 1);
                client.disconnect();
                console.log('Will Remove From Server List');
            }
        }
    }


    client
        .submitJob("count", JSON.stringify([input, startTime]))
        .then(function (result) {
            client.removeListener('disconnect', disconnectFunction);
            let [count, startTime] = JSON.parse(result);
            let endTime = Date.now();
            let time = endTime - startTime;
            console.log(`------------COUNT : ${time}ms elapsed------------`);

            /* update output */
            if (AVmode !== 2) {
                for (let k = 0; k <= 100; k++) {
                    output[k][1] += count[k][1];
                }
            } else {
                countTemp.push(count);
            }
            console.log(count);
            /* count finish */
            finish--;

            /* check final output */
            if (finish == 0) {
                if (AVmode == 2) {
                    let countOut = [];
                    let tmp;
                    //console.log(countTemp[0][0]);
                    for (let i = 0; i < countTemp[0].length; i++) { // i번째 점수
                        let scoreCount = new Map();
                        let scoreNum = countTemp[0][i][0];
                        for (let j = 0; j < countTemp.length; j++) { // j 번째 input count
                            if (scoreCount.has(countTemp[j][i][1])) {
                                tmp = scoreCount.get(countTemp[j][i][1]);
                                scoreCount.delete(countTemp[j][i][1]);
                                scoreCount.set(countTemp[j][i][1], tmp + 1);
                            } else
                                scoreCount.set(countTemp[j][i][1], 1);
                        }
                        scoreCount = [...scoreCount.entries()].sort();
                        countOut.push([scoreNum, scoreCount[scoreCount.length - 1][0]]);
                    }
                    output = countOut;
                }
                console.log("---------------FINAL OUTPUT---------------");
                console.log(output);
            }
        });
    client.on('disconnect', disconnectFunction);
}

async function execute() {
    finish = taskNum;

    while (finish != 0) {
        executeSum();
        executeCount();
        await timeout(1); // make CPU idle (context switch for callback function)
    }

    verifyResult();
    console.log("PROGRAM EXIT");
}

function verifyResult() {
    var sum = 0;
    for (var i = 0; i < output.length; i++) {
        sum += output[i][1];
    }
    console.log("---------------VERIFY RESULT---------------");
    console.log(`studentNum = ${studentNum} / sum of output = ${sum}`);
    console.log(studentNum == sum ? "PASS" : "FAIL");
}

async function program() {
    setMode();
    connectServers();
    while (serverList.length < nodeNum) await timeout(1);
    console.log(serverList.length);
    setValues();
    initializeOutput();
    execute();
}

program();