const crawlTimeIr = require("./pipe2time.ir")
const buildICS = require("./pipe2calendar")
const inquirer = require("inquirer")


var myArgs = process.argv.slice(2);
var years = myArgs[0];
var ics = false;
var api = true;

(async () => {
	console.log("hello")
	console.log(years)
	console.log(ics)
	console.log(api)
	const yParse = years.split(",")
	yParse.map(year => {
		if (!(+year <= 1500 && +year >= 1280)) {
			throw new Error({
				code: "YEAR_VALIDATION",
				message: `Input year does not in range of support. year must grather than 1280 and less than 1500, input year => ${year}`,
			})
		}
	})
	if (yParse.length) await crawlTimeIr(yParse, api)
	if (ics) yParse.map(year => buildICS(year))
})()