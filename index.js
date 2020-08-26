const AWS = require("aws-sdk");
const archiver = require('archiver');
const stream = require('stream');
const util = require('util');

const s3 = new AWS.S3();


const S3_ZIP_FOLDER = "user-proofs-zips";


//This returns us a stream.. consider it as a real pipe sending fluid to S3 bucket.. Don't forget it
const streamTo = (bucket, key, streamPassThrough) => {

	return s3.upload( { Bucket: bucket,
		Key: key,
		ACL: 'public-read',
		ContentType: 'application/zip',
		Body: streamPassThrough }, (err, data) => {
		if(err) {
			console.log("Error streaming data to S3. Error=" + err);
		}
 	} );
};

const generateRandomString = function(length) {
    const chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz';
    let randomString = '';
    for (let i = 0; i < length; i++) {
        const rnum = Math.floor(Math.random() * chars.length);
        randomString += chars.substring(rnum, rnum + 1);
    }
    return randomString;
};

const createFileName = function(name) {
	if(name.indexOf(".") === -1) {
		return name + '.jpg';
	} else {
		return name;
	}
}

exports.handler = async (event, context, callback) => {

	console.log("Zipping files for event="+ JSON.stringify(event));
	const bucket = event.s3Bucket;

	const zipFileKeys = {};

	for(const userId in event.userProofsByUserIds) {

		try {

			const imageKeys = [];
			if(event.userProofsByUserIds.hasOwnProperty(userId)) {
				const userProofs = event.userProofsByUserIds[userId];
				userProofs.forEach(userProof=> {
					imageKeys.push(userProof.imageUrlS3Key);
					if(userProof.otherSideImageS3Key) {
						imageKeys.push(userProof.otherSideImageS3Key);
					}
					if(userProof.aadharPdfUrl) {
						imageKeys.push(userProof.aadharPdfUrl);
					}
				});
			}


			const s3GetObjectAsync = util.promisify(s3.getObject).bind(s3);

		    const list = await Promise.all(imageKeys.map(key => new Promise((resolve, reject) => {
		            s3GetObjectAsync({Bucket:event.s3Bucket, Key:key})
		                .then(data => resolve( { data: data.Body, name: createFileName(`${key.split('/').pop()}`) } ));
		        }
		    ))).catch(err => {
		    	console.log(err);
		    	throw new Error(err)
		    });

		    const zipKey = `${S3_ZIP_FOLDER}/${userId}_${generateRandomString(6)}.zip`;

	       	const archive = archiver('zip');
	        archive.on('error', err => { throw new Error(err); } );

	        const  streamPassThrough = new stream.PassThrough();

	       	const s3Upload = streamTo(bucket, zipKey, streamPassThrough);

		    s3Upload.on('httpUploadProgress', progress => {
			        console.log(progress);
			    });

		    await new Promise((resolve, reject) => {
		        s3Upload.on('close', resolve());
		        s3Upload.on('end', resolve());
		        s3Upload.on('error', reject());

		        archive.pipe(streamPassThrough);
		        list.forEach(itm => archive.append(itm.data, { name: itm.name }));
		        archive.finalize();
		    }).catch(err => {
		    	console.log(err);
		    	throw new Error(err);
		    });

		 	await s3Upload.promise();

		    zipFileKeys[userId] = zipKey;
		} catch(err) {
			console.log(`Error creating zipFile for userId=${userId}`);
		}

	}

    const response = {
        statusCode: 200,
        body: JSON.stringify({
        	zipFiles: zipFileKeys
        }),
    };
    return response;
};
