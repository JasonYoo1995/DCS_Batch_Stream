var gearmanode = require("gearmanode");
var exec = require("child_process").exec;
//var delay = 5;
var delay = 100;
var die = 0;

// sleep
function timeout(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

var port = 4730 + Number(process.argv[2]);
var worker = gearmanode.worker({ port: port });

worker.addFunction("sum", sumFunction);
worker.addFunction("count", countFunction);
worker.on('socketDisconnect', disconnectFunction);

async function sumFunction(job) {
    var [input, startTime] = JSON.parse(job.payload);
    var output = [];
    for (var i = 0; i < input.length; i++) {
        var temp = input[i];
        var sum = 0;
        for (var j = 0; j < temp.length; j++) {
            if (die)
                return;
            sum += temp[j][1];
            await timeout(delay);
            console.log(
                `SUM : ${i + 1}/${input.length} ${j + 1}/${temp.length
                } --- ${delay}ms per 1 operation`
            );
        }
        output.push([temp[0][0], sum]); // [studentId, totalScore]
    }
    console.log("------------SUM OUTPUT------------");
    console.log(output);
    job.workComplete(JSON.stringify([output, startTime]));
}

async function countFunction(job) {
    var [input, startTime] = JSON.parse(job.payload);
    var output = [];
    for (var i = 0; i <= 100; i++) {
        output.push([i, 0]);
    }
    for (var i = 0; i < input.length; i++) {
        if (die)
            return;
        output[input[i][1]][1]++;
        await timeout(1000);
        console.log(
            `COUNT ${i + 1}/${input.length} --- ${delay}ms per 1 operation`
        );
    }
    console.log("------------COUNT OUTPUT------------");
    console.log(output);
    job.workComplete(JSON.stringify([output, startTime]));
}

// Worker FT
function disconnectFunction() {
    die = true;
    console.log('[ERROR] Server Died. Trying to Restart Server');
    let command = 'pm2 start -f ' + __dirname + '/server.js -- ' + process.argv[2];
    exec(command, async function (error, stdout, stderr) {
        if (error == null) {
            console.log('----Successfully Restarted Server----');
            await timeout(1000); // Time to Start pm2 service
            die = false;
            // Reset Server
            worker = gearmanode.worker({ port: port });
            worker.addFunction("sum", sumFunction);
            worker.addFunction("count", countFunction);
            worker.on('socketDisconnect', disconnectFunction);
        } else {
            console.log("exec error: " + error);
        }
    });
}