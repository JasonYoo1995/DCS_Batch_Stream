var gearmanode = require("gearmanode");

var delay = 5;

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

var port = 4730 + Number(process.argv[2]);
var worker = gearmanode.worker({ port: port });

worker.addFunction("sum", function (job) {
  var [input, startTime] = JSON.parse(job.payload);
  var output = [];
  for (var i = 0; i < input.length; i++) {
    var temp = input[i];
    var sum = 0;
    for (var j = 0; j < temp.length; j++) {
      sum += temp[j][1];
      timeout(delay);
      console.log(
        `SUM : ${i + 1}/${input.length} ${j + 1}/${
          temp.length
        } --- ${delay}ms per 1 operation`
      );
    }
    output.push([temp[0][0], sum]); // [studentId, totalScore]
  }
  console.log("------------SUM OUTPUT------------");
  console.log(output);
  job.workComplete(JSON.stringify([output, startTime]));
});

worker.addFunction("count", function (job) {
  var [input, startTime] = JSON.parse(job.payload);
  var output = [];
  for (var i = 0; i <= 100; i++) {
    output.push([i, 0]);
  }
  for (var i = 0; i < input.length; i++) {
    output[input[i][1]][1]++;
    timeout(delay);
    console.log(
      `COUNT ${i + 1}/${input.length} --- ${delay}ms per 1 operation`
    );
  }
  console.log("------------COUNT OUTPUT------------");
  console.log(output);
  job.workComplete(JSON.stringify([output, startTime]));
});
