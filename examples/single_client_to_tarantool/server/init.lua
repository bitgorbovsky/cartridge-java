------------------------------------------------------------------------------
-- Single tarantool server
------------------------------------------------------------------------------
local log = require 'log'


box.cfg {
    listen = 3301
}


box.once('schema-0.0.1', function()
    box.schema.space.create('users', {
        if_not_exists = true,
        format = {
            { 'id', type = 'number' },
            { 'name', type = 'string', is_nullable = true }
        }
    })
    box.space.users:create_index('pk', {
        parts = { 'id' }
    })

    box.schema.user.create('authenticator', {
        password = 'secret',
        if_not_exists = true
    })
    box.schema.user.grant(
        'authenticator',
        'read,write,execute',
        'universe',
        nil,
        { if_not_exists = true }
    )
    log.info('schema created')
end)


log.info('app configured')
