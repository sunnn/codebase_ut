import configparser
def cfgparse(cfgparam):
    cfgparser = configparser.ConfigParser()
    cfgparser.read(cfgparam)
    section = 'default'
    dictionary = {}
    for section in cfgparser.sections():
        dictionary[section] = {}
        for option in cfgparser.options(section):
            dictionary[section][option] = cfgparser.get(section, option)
    return dictionary