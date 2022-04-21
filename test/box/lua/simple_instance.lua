box.cfg{}

require('fiber').set_default_slice(10000)
require('console').listen(os.getenv('ADMIN'))
