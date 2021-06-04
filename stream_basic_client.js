var Gearman = require("abraxas");

var studentNum = 30;
var problemNum = 25;
var nodeNum = 3;
var inputPerBatch = 7;

var score = [];
var sumInput = [];
var countInput = [];

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

function setDividingValues() {
  quotient = Math.floor(score.length / inputPerBatch);
  remainder = score.length % inputPerBatch;
}

function getRandomScore() {
  return Math.floor(Math.random() * 5);
}

function initializeInput() {
  for (let i = 0; i < studentNum; i++) {
    var temp = [];
    for (let j = 0; j < problemNum; j++) {
      temp.push([i, getRandomScore()]); // [student ID, score]
    }
    score.push(temp);
  }
}

function initializeOutput() {
  for (var i = 0; i <= 100; i++) {
    output.push([i, 0]);
  }
}

function makeJobIntoTask() {
  for (var i = 0; i <= quotient; i++) {
    var input = [];
    for (var j = 0; j < inputPerBatch; j++) {
      if (i == quotient && j == remainder) break;
      input.push(score[i * inputPerBatch + j]);
    }
    sumInput.push(input);
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
  sumRRpointer %= nodeNum;

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
    });
}

function executeCount() {
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
  finish = Math.floor((score.length - 1) / inputPerBatch) + 1;
  // all jobs are completed if finish is 0

  while (finish != 0) {
    executeSum();
    executeCount();
    await timeout(1); // prevent busy waiting (make idle for callback function)
  }

  console.log("PROGRAM EXIT");
}

initializeInput();
// console.log(score);

setDividingValues();
// console.log(quotient);
// console.log(remainder);

makeJobIntoTask();
// for (var i = 0; i < sumInput.length; i++) {
//   console.log(sumInput[i]);
// }

initializeOutput();
// console.log(output);

executeBatch();
