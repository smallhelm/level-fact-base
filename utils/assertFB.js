module.exports = function(fb){
  if(!fb || !fb.hindex || !fb.db || !fb.schema || !fb.types){
    throw new Error("Must pass fb as the first argument");
  }
};
