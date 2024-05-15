// Util functions for working with Github images.

axios = require('axios');
const { Octokit } = require("@octokit/rest");
const octokit = new Octokit({ auth: process.env.GITHUB_PERSONAL_ACCESS_TOKEN });

// Added a function to store the image.
async function storeImage(url, path, name) {
    try {
        const base64Image = await downloadImage(url);

        return await imageUploadToRepo(base64Image, path, name);
    } catch (error) {
        console.error('Error in executing image operations:', error);
    }
}

// Added a function to download an image from the URL.
async function downloadImage(url) {
    try {
        const response = await axios({
            url: url,
            responseType: 'arraybuffer'
        });
    
        return Buffer.from(response.data, 'binary').toString('base64');
    } catch(err) {
        console.log('Downloading Image', err.msg, url)
    }
}


// Function to upload the image
async function imageUploadToRepo(content, destPath, destName) {
    try {
        // Se the message and the content path.
        const message = `Added Image for ${destPath}`;
        const contentPath = `${destPath}/${destName}`;

        const repo = process.env.GITHUB_REPO;
        const owner = process.env.GITHUB_OWNER;
        const branch = process.env.GITHUB_BRANCH;

        console.log(repo, owner, branch);

        // Get the reference for the branch
        const ref = await octokit.rest.git.getRef({
            owner,
            repo,
            ref: `heads/${process.env.GITHUB_BRANCH}`
        });

        const sha = ref.data.object.sha;

        // Create blob
        const blob = await octokit.rest.git.createBlob({
            owner,
            repo,
            content,
            encoding: 'base64'
        });

        // Get the latest commit
        const commit = await octokit.rest.git.getCommit({
            owner,
            repo,
            commit_sha: sha
        });

        // Create tree
        const tree = await octokit.rest.git.createTree({
            owner,
            repo,
            tree: [{
                path: contentPath,
                mode: '100644',
                type: 'blob',
                sha: blob.data.sha
            }],
            base_tree: commit.data.tree.sha
        });

        // Create new commit
        const newCommit = await octokit.rest.git.createCommit({
            owner,
            repo,
            message,
            tree: tree.data.sha,
            parents: [commit.data.sha]
        });

        // Update branch reference
        await octokit.rest.git.updateRef({
            owner,
            repo,
            ref: `heads/${branch}`,
            sha: newCommit.data.sha
        });

        console.log(`File uploaded successfully! (${destName})`);
        return;

    } catch (error) {
        console.error('Error uploading image:', error, `file: ${destName}`);
    }
}

module.exports = {
    downloadImage,
    storeImage,
    imageUploadToRepo
}