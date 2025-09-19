{
  newComposeService(options):: {
    [options.name]: { image: options.image } + 
    (if options.command then { command: options.command } else null) +
    (if options.volumes then { volumes: options.volumes } else null) +
    (if options.ports then { volumes: options.ports } else null)
  },

  newComposeFile(options):: {
    services: 
      options.services
  }
}