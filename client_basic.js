var Gearman = require("abraxas");

var batchMode = false;
var streamMode = false;

var nodeNum = 3;
var score = [];

var taskNum = 0;

var sumInput = [];
var countInput = [];

var sumOutputNum = 0;

var sumRRpointer = 0;
var countRRpointer = 0;

var finish;
var output = [];

// function runShellCommand(command) {
//   var exec = require("child_process").exec;
//   exec(command, function (error, stdout, stderr) {
//     console.log("stdout: " + stdout);
//     console.log("stderr: " + stderr);
//     if (error !== null) {
//       console.log("exec error: " + error);
//     }
//   });
// }

// sleep
function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function setMode() {
  var arg = Number(process.argv[2]);
  if (arg == 0) batchMode = true;
  else if (arg == 1) streamMode = true;
}

function setValues() {
  var studentNum = 31;
  var problemNum = 3; // must be equal or less than 25
  var quotient;
  var remainder;
  var inputPerBatch = 7; // for stream mode

  for (let i = 0; i < studentNum; i++) {
    var temp = [];
    for (let j = 0; j < problemNum; j++) {
      temp.push([i, getRandomScore()]); // [student ID, score]
    }
    score.push(temp);
  }

  if (batchMode) {
    // studentNum   tasknum = nodeNum  quotient    (quotient+remainder)
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

  // load-balancing strategy : round-robin
  let client = Gearman.Client.connect({
    servers: ["127.0.0.1:" + (sumRRpointer + 4730)],
    defaultEncoding: "utf8",
  });
  sumRRpointer++;
  sumRRpointer %= nodeNum; // control index

  // execute
  let input = sumInput[0];
  sumInput.splice(0, 1); // remove item on index 0
  let startTime = Date.now();
  client
    .submitJob("sum", JSON.stringify([input, startTime]))
    .then(function (result) {
      let [sum, startTime] = JSON.parse(result);
      let endTime = Date.now();
      let time = endTime - startTime;
      console.log(`------------SUM : ${time}ms elapsed------------`);
      countInput.push(sum);
      sumOutputNum++; // for batch mode
    });
}

function executeCount() {
  if (batchMode && sumOutputNum < taskNum) return;
  if (countInput.length == 0) return;

  // load-balancing strategy : round-robin
  let client = Gearman.Client.connect({
    servers: ["127.0.0.1:" + (countRRpointer + 4730)],
    defaultEncoding: "utf8",
  });
  countRRpointer++;
  countRRpointer %= nodeNum;

  // execute
  let input = countInput[0];
  countInput.splice(0, 1); // remove item on index 0
  let startTime = Date.now();
  client
    .submitJob("count", JSON.stringify([input, startTime]))
    .then(function (result) {
      let [count, startTime] = JSON.parse(result);
      let endTime = Date.now();
      let time = endTime - startTime;
      console.log(`------------COUNT : ${time}ms elapsed------------`);

      /* update output */
      for (let k = 0; k <= 100; k++) {
        output[k][1] += count[k][1];
      }

      /* count finish */
      finish--;

      /* check final output */
      if (finish == 0) {
        console.log("---------------FINAL OUTPUT---------------");
        console.log(output);
      }
    });
}

async function executeBatch() {
  finish = taskNum;

  while (finish != 0) {
    executeSum();
    executeCount();
    await timeout(1); // make CPU idle (context switch for callback function)
  }
  console.log("PROGRAM EXIT");
}

function program() {
  setMode();
  setValues();
  initializeOutput();
  executeBatch();
}

program();
