"use strict";
/*
	Portal-specific functions
*/

let serverTZ = "UTC",
	localTz = moment.tz.guess(),
	generalDateFormat="YYYY-MM-DDTHH:mm:ss",
	chartsQueries = [
		{ 
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_electricity_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`, 
			"columnName": "electric",
			"queryType": "Electricity" 
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_occupancy_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "occupancy",
			"queryType": "Occupancy"
		}, 
		{
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_temperature_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_temperature",
			"queryType": "Ambient_Temperature"
		}, 
		{
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_wind_direction_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_wind_direction",
			"queryType": "Ambient_Wind_Direction"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_wind_speed_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_wind_speed",
			"queryType": "Ambient_Wind_Speed"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_humidity_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_humidity",
			"queryType": "Ambient_Humidity"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_cloud_cover_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_cloud_cover",
			"queryType": "Ambient_Cloud_Cover"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_ambient_dew_point_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_dew_point",
			"queryType": "Ambient_Dew_Point"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_average_grid_emissions_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_grid_emissions",
			"queryType": "Average_Grid_Emissions"
		}, {
			"template": (property_uri, start_date, end_date) => `
				SELECT *
					FROM \`standardized_new.portal_properties_marginal_grid_emissions_view\`
					WHERE property_uri='${property_uri}'
						AND timestamp BETWEEN "${start_date}" AND "${end_date}"
					ORDER BY TIMESTAMP DESC
			`,
			"columnName": "sum_grid_emissions",
			"queryType": "Marginal_Grid_Emissions"
		}		
	],
	chartTypes = _.map(chartsQueries, value => {
		return _.result(value, "queryType")
	}),
	dashboardUrl = (
		'https://datastudio.google.com/embed/reporting/' + 
		'c1a3bf5e-ae01-4ca5-b05f-0127fbcc82b6/page/p_nsipm3ylzc'
	);

chartTypes = _(chartTypes).sortBy().value();

function getIconByConfigType(configType){
	var mapper = {
		'Ambient Cloud Cover': 'mdi:apple-icloud',
		'Ambient Dew Point': 'mdi:water-thermometer-outline',
		'Ambient Humidity': 'mdi:cloud-percent',
		'Ambient Real Feel Temperature': 'carbon:temperature-feels-like',
		'Ambient Temperature': 'carbon:temperature-fahrenheit',
		'Ambient Wind Direction': 'mdi:windsock',
		'Ambient Wind Speed': 'mdi:wind-power',
		'Average Grid Emissions': 'tabler:brand-carbon',
		'Marginal Grid Emissions': 'mdi:molecule-co2',
		'Occupancy': 'mdi:account-plus',
		'Electric': 'mdi:lightning-bolt-outline'
	},
	default_icon = 'carbon:not-available';

	return _.get(mapper, configType, default_icon);
}


function makeFileName() {
	var startDate = new Date()
	startDate.setDate(startDate.getDate() - 1)
	return startDate.toISOString().substring(0, 19)
}


function startOffsetRawPullOrStandardizeFileName(
	currDate=undefined,
	offsetValue=1,
	offsetType="h",
	dateFormat=generalDateFormat,
) {
	var nowDate = currDate ? currDate : moment().minutes(0).seconds(0),
		startDate = moment(nowDate).subtract(offsetValue, offsetType);

	return startDate.format(dateFormat);
}

function endOffsetRawPullOrStandardizeFileName(
	currDate=undefined,
	offsetValue=1,
	offsetType="h",
	dateFormat=generalDateFormat,
) {
	var nowDate = currDate ? currDate : moment().minutes(0).seconds(0),
		startDate = moment(nowDate).add(offsetValue, offsetType);

	return startDate.format(dateFormat);
}


// function getBasename(prevname) {
// 	return prevname.replace(/^(.*[/\\])?/, '').replace(/(\.[^.]*)$/, '');
// };

function childHaystackTags($xml, path) {
	try {
		return $xml.find(path).children().map((idx, tag) => {
			return tag.localName + ': ' + tag.textContent
		}).toArray()
	} catch (err) {
		return `Error. XML needs validation against XML Schema - ${err}`
	}
}

async function modelParticipant(pModel) {
	console.log('Start modeling participant ' + pModel.bucket)
	const configFiles = await listFiles(
		pModel.bucket, 
		'config/', 
		'', 
		'~'
	),
	codeFiles = await listFiles(
		pModel.bucket, 
		'config/meter_provider_auth_codes', 
		'', 
		'~'
	),
	codeObjs = _.filter(codeFiles, obj => !obj.name.endsWith("/")),
	metersObjs = _.filter(configFiles, obj=> /^config\/meter_.*\.xml$/.test(obj.name)),
	participantObject = _.filter(configFiles, obj=> /^config\/participant\.xml$/.test(obj.name));
	// ===== codes
	console.log(
		`Processing connectors configs files of ${pModel.bucket} - `, 
		codeObjs
	);

	const codePromises = await codeObjs.map(async (codeObj, index) => {
		await extendedAttempt(
			async (codeCfg) => {
				return {
					content: await getObject(
						codeCfg.bucket, 
						codeCfg.name
					),
					cfg: codeCfg
				}
			},
			codeObj
		).then(
			async (result) => {
				pModel.codes.push({
					name: result.cfg.name,
					baseName: getBasename(result.cfg.name),
					content: result.content,
					cfg: result.cfg
				});
				return result;
			}
		).catch(err => {
			console.error(err.message);
		});
	});
	await Promise.all(codePromises);
	console.info(
		`Finished processing connectors configs files of ${pModel.bucket}`
	);

	console.info(`Start processing meters configuration files ${pModel.bucket}`);
	const promises = await metersObjs.map(async (meterObj, index) => {
		var pMeter = {
			idx: `${meterObj.bucket}-meter-${index}`,
			bucket: meterObj.bucket,
			fileName: meterObj.name,
			validation: []
		};
		pModel.meters.push(pMeter)
		await extendedAttempt(
			async (meter) => {
				return await getObject(
					meter.bucket, 
					meter.fileName
				)
			},
			pMeter
		).then(
			async (data) => {
				let meterXML = $($.parseXML(data));
				await modelMeters(meterXML, pMeter); 
				return data;
			}
		).catch(err => {
			console.error(err.message);
			pMeter.validation.push(`Error: ${err.message}`);
			pMeter.status = 'error';
			if (!pMeter.type || pMeter.type.length == 0) {
				pMeter.type = 'undefined';
			}
		});
	});

	await Promise.all(promises);
	console.info(`Finished processing meters configuration files of ${pModel.bucket}`);


	// pushes
    // Here one element array with participant.xml config or empty one
	console.info(`Start processing participant configuration file of ${pModel.bucket}`);
	const ptcpnt_promises = await participantObject.map(async (participantObj, index) => {
		await extendedAttempt(
			async (participant) => {
				return await getObject(
					participant.bucket, 
					participant.name
				)
			},
			participantObj
		).then(
			async (participantRawXml) => {
				let participantXml = $($.parseXML(participantRawXml));
				
				pModel.name = participantXml.find(
					'hbd\\:participant hbd\\:name'
				).text();

				if (pModel.name === '') {
					throw Error(
						'Error: missing participant name in participant.xml;' + 
						' validate it against XML Schema'
					);
				}
				pModel.pushes = await modelPushes(pModel.bucket, participantXml);
				console.info(
					`Processing participant properties files ` + 
					`of ${pModel.bucket}`
				);			
				await extendedAttempt(
					async (participantXmlObj, env) => {
						return await modelProperties(participantXmlObj, env);
					},
					participantXml,
					pModel.env
				).then(
					(data) => {
						pModel.properties = data;
						console.info(
							`Finished processing participant properties files ` + 
							`of ${pModel.bucket}`
						);
					} 
				).catch(err => {
					console.error(err.message);
					pModel.validation.push(
						`Error while processing properties: ${err}`);
					pModel.status = 'error';
				});

			}
		).catch(err => {
			console.error(err.message);
			pMeter.validation.push(
				`Error in push/pull config in participant.xml: ${err.message}`
			);
			pMeter.status = 'error';
		});
	});
	await Promise.all(ptcpnt_promises);
	console.info(
		`Finished processing participantconfiguration file of ${pModel.bucket}`
	);
}

async function modelMeters($meterXml, pMeter) {
	pMeter.uri = $meterXml.find('hbd\\:meterURI').text();
	pMeter.short_uri = getBasename($meterXml.find('hbd\\:meterURI').text());
	pMeter.type = $meterXml.find('hbd\\:type').text();
	pMeter.icon_type = getIconByConfigType(pMeter.type);
	pMeter.updateFrequency = $meterXml.find('hbd\\:updateFrequency').text();
	const stdBucket = $meterXml.find('hbd\\:meteredDataLocation').attr('hbd:bucket'),
		stdPath = $meterXml.find('hbd\\:meteredDataLocation').attr('hbd:path');

	pMeter.stdFiles = []

	var dateNow = moment().startOf('hour').tz(localTz),
		serverDateNow=moment.tz(dateNow, serverTZ),
		
		startOffsetFileName = startOffsetRawPullOrStandardizeFileName(
			serverDateNow, 
			1, 
			"months"
		),
		endOffsetFileName = endOffsetRawPullOrStandardizeFileName(
			serverDateNow, 
			1, 
			"months"
		);

	const allFiles = await listFiles(
		stdBucket, 
		stdPath, 
		`${stdPath}/${startOffsetFileName}`,
		`${stdPath}/${endOffsetFileName}`
	)
	//console.log(allFiles)
	try {
		let stdFiles = allFiles.map(
			x => {
				return {
					'baseName': getBasename(x.name),
					'name': x.name,
					'bucket': x.bucket
				}
			}
		).filter(
			x => moment(x.baseName, generalDateFormat, true).isValid()
		);

		pMeter.stdFiles = _.take(
			_(stdFiles).sortBy().reverse().value(),
			12
		);
		pMeter.stdFiles.sort();
		pMeter.stdFiles.reverse();
	} catch (err) {
		throw Error(`Fetching standardized data files from ${stdBucket}/${stdPath}`)
	}
	
	if (pMeter.stdFiles && pMeter.stdFiles.length) {
		pMeter.lastStdUpdate = moment(pMeter.stdFiles[0].baseName, generalDateFormat, true);
		pMeter.lastStdUpdate.tz(serverTZ, true);

		var localLastUpdate = moment.tz(pMeter.lastStdUpdate, localTz);

		pMeter.duration = moment.duration(dateNow.diff(localLastUpdate)) 
		pMeter.lastUpdateHours = pMeter.duration.hours();
		pMeter.lastUpdateHoursHumanized = pMeter.duration.humanize();

		var allowedDelay = pMeter.updateFrequency == 'Monthly' ? 32 : 2;
		// TODO: hourly files in last 24 hours

		pMeter.status = 'success'
		if (pMeter.lastUpdateHours > allowedDelay) {
			pMeter.status = 'warning'
			pMeter.validation.push('Stale data')
		} else if (localLastUpdate.isAfter(dateNow)) {
			pMeter.status = 'danger'
			pMeter.validation.push('Future data')
		}
	} else {
		pMeter.status = 'danger';
		pMeter.validation.push('No valid data file name found in ' + stdBucket + '/' + stdPath);
		pMeter.lastUpdateHours = 10000;
		pMeter.lastUpdateHoursHumanized = 'A long time';
	}

	if (pMeter.uri !== pMeter.bucket + '/' + pMeter.fileName) {
		pMeter.validation.push(
			`URI (${pMeter.uri}) is not the same as meter def. ` + 
			`XML file name, expected (${pMeter.bucket}/${pMeter.fileName}).`
		)
		pMeter.status = 'error'
	}
	// end check meter status
	
	pMeter.haystack = childHaystackTags($meterXml, 'hbd\\:meter hbd\\:tags haystack\\:haystack')

}

async function modelPushes(bucket, $participantXml) {
	var pushes = []
	const descriptions = $participantXml.find(
		`hbd\\:participant hbd\\:connectors hbd\\:connector ` +
		`hbd\\:fetchStrategy hbd\\:push hbd\\:description`
	)
	for (var idx = 0; idx < descriptions.length; idx++) {
			const element = descriptions[idx],
				description = $(element).text(),
				connector = $(element).parent().parent().parent(),
				connectorName = connector.find('hbd\\:function').text(),
				rawBucket = connector.children(
					'hbd\\:rawDataLocation'
				).attr(
					'hbd:bucket'
				),
				rawPath = connector.children(
					'hbd\\:rawDataLocation'
				).attr(
					'hbd:path'
				),
				allFiles = await listFiles(
					rawBucket, 
					`${rawPath}/raw-data-`, 
					`${rawPath}/ `, 
					`${rawPath}/~`
				);


			// const meter_config = await getObject(
			// 	rawBucket, 
			// 	participant.name
			// );

			var meters_uri = connector.children('hbd\\:meterURI').map(
					(idx, elt) => _.replace($(elt).text(), `${rawBucket}/`, '')
				).get(),
				meters_info = [];

			const codePromises = await meters_uri.map(
				async (meterUri, index) => {
					await extendedAttempt(
						async (meterUri) => {
							return await getObject(rawBucket, meterUri)
						},
						meterUri
					).then(
						async (result) => {
							let meterXML = $($.parseXML(result)), 
								type = meterXML.find('hbd\\:type').text()
							meters_info.push({
								meterUri: getBasename(meterUri),
								type: type,
								icon_type: getIconByConfigType(type)
							});
							return result
						}
					).catch(err => {
						console.error(err.message);
					});
				}
			);
			await Promise.all(codePromises);

			var push = {
				idx: bucket + idx,
				meterURIs: meters_info,
				timezone: connector.children('hbd\\:timezone').first().text(),
				connectorName: connectorName,
				description: description,
				bucket: rawBucket,
				fileName: rawPath + '/raw-data',
				uploads: [],
				validation: []
			}
			if (allFiles) {
				push.uploads = allFiles.map(file => file.name.substring(rawPath.length + 1))
			}
			pushes.push(push)
	}
	return pushes
}


async function modelProperties($participantXml, dataset) {
	var properties = [],
		ds = null;
	try {
		ds = $participantXml.find('hbd\\:participant hbd\\:properties hbd\\:propertyURI').toArray()
	} catch (err) {
		console.log(err)
		throw Error('Nothing found under hbd:participant/hbd:properties/hbd:propertyURI')
	}

	if (!ds) {
		throw Error('Error under hbd:participant/hbd:properties/hbd:propertyURI')
	}
	
	for (var idx in ds) {
		var pModel = null
		try {
			const propertyUri = $(ds[idx]).text(),
				propertyXmlObject = {
				'bucket': propertyUri.substring(0, propertyUri.indexOf('/')),
				'name': propertyUri.substring(propertyUri.indexOf('/') + 1)
			};
			
			let params = encodeURIComponent(
				JSON.stringify(
					{
					  	"ds0.property_uri": `${propertyXmlObject.bucket}/${propertyXmlObject.name}`,
					  	"ds181.property_uri": `${propertyXmlObject.bucket}/${propertyXmlObject.name}`,
					  	"ds182.property_uri": `${propertyXmlObject.bucket}/${propertyXmlObject.name}`,
					}
				)
			)


			pModel = {
				idx: propertyXmlObject.bucket + '_' + idx,
				uri: propertyUri,
				status: 'success',
				validation: [],
				bucket: propertyXmlObject.bucket,
				fileName: propertyXmlObject.name,
				dataStudioLink: `${dashboardUrl}?params=${params}`,
			}

			_.forEach(chartTypes, queryType => {
				pModel[`data_${_.toLower(queryType)}`] = [];
			});
			console.log("sdzxv");
		} catch (err) {
			console.log(err)
			throw Error('Parsing property ' + propertyUri)			
		}

		try {
			const $propertyXml = $($.parseXML(await getObject(pModel.bucket, pModel.fileName)))

			const addressElement = $propertyXml.find('hbd\\:property espm\\:address').first()
			pModel.grossFloorArea = $propertyXml.find('hbd\\:property espm\\:grossFloorArea espm\\:value').first().text()
			pModel.address = addressElement.attr('address1') + ', ' + addressElement.attr('address2') + ', ' + addressElement.attr('city')
			pModel.name = $propertyXml.find('espm\\:name').text()
			pModel.haystack = childHaystackTags($propertyXml, 'hbd\\:property hbd\\:tags haystack\\:haystack')
		} catch (err) {
			console.log(err)
			throw Error('Error fetching property ' + pModel.uri + ':' + err)
		}

		var weightsQuery = `
			SELECT *
			FROM \`standardized_new.portal_weights_view\`
			WHERE property_uri = '${pModel.uri}'
			ORDER BY meter_uri ASC		
		`

		console.log("====================================================");
		console.log(`weightsQuery - ${weightsQuery}`);
		console.log("====================================================");
		const weightsData = await query(dataset, weightsQuery)
		if (_.isEmpty(weightsData)){
			pModel.status = 'warning';
			pModel.validation.push('No meter weights defined in BigQuery DW');			
		}
		pModel.weights = _.map(weightsData, 
			(value) => {
				return {
					type: value.type,
					icon_type: getIconByConfigType(value.type),
					property_uri: value.property_uri,
					meter_uri: getBasename(value.meter_uri),
					weight: parseFloat(value.weight),
				}
			}
		);

		try {
			let date_format = "YYYY-MM-DDTHH:mm:ss",
				end_date = moment().minutes(0).seconds(0),
				start_date = moment(end_date).subtract(72, 'h'),
				end_date_str = end_date.format(date_format),
				start_date_str = start_date.format(date_format);

			const promises = await chartsQueries.map(async (queryInfo) => {
				let dtQuery = queryInfo.template(
					pModel.uri,
					start_date_str,
					end_date_str
				);
				const queryData = await query(dataset, dtQuery);
				console.log('---------------------------------------------');
				console.log(`${queryInfo.queryType}, ${queryInfo.columnName} - ${dtQuery}`);
				console.warn('---------------------------------------------');
				_.map(queryData, row => {
					pModel[`data_${_.toLower(queryInfo.queryType)}`].push([
						new Date(row.timestamp).getTime(),
						parseFloat(row[queryInfo.columnName])
					])
				});

			});
			await Promise.all(promises);				

		} catch (err) {
			console.log(err + ' at ' + pModel.uri)
			console.log(err)
			throw Error(
				`Error fetching property representative values from BigQuery` + 
				` for property '${pModel.uri}':' ${err}`
			)
		}
		properties.push(pModel)
	}
	return properties

}
