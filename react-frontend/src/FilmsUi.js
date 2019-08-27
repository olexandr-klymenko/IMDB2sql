import React from 'react';
import PropTypes from 'prop-types';
import Downshift from 'downshift';
import {makeStyles} from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';
import Paper from '@material-ui/core/Paper';
import MenuItem from '@material-ui/core/MenuItem';
import Chip from '@material-ui/core/Chip';
import ApolloClient, {gql} from 'apollo-boost';
import {debounce, throttle} from "throttle-debounce";


function renderInput(inputProps) {
    const {InputProps, classes, ref, ...other} = inputProps;

    return (
        <TextField
            InputProps={{
                inputRef: ref,
                classes: {
                    root: classes.inputRoot,
                    input: classes.inputInput,
                },
                ...InputProps,
            }}
            {...other}
        />
    );
}

renderInput.propTypes = {
    /**
     * Override or extend the styles applied to the component.
     */
    classes: PropTypes.object.isRequired,
    InputProps: PropTypes.object,
};

function renderSuggestion(suggestionProps) {
    const {suggestion, index, itemProps, highlightedIndex, selectedItem} = suggestionProps;
    const isHighlighted = highlightedIndex === index;
    const isSelected = (selectedItem || '').indexOf(suggestion.title) > -1;

    return (
        <MenuItem
            {...itemProps}
            key={suggestion.title}
            selected={isHighlighted}
            component="div"
            style={{
                fontWeight: isSelected ? 500 : 400,
            }}
        >
            {suggestion.title}
        </MenuItem>
    );
}

renderSuggestion.propTypes = {
    highlightedIndex: PropTypes.oneOfType([PropTypes.oneOf([null]), PropTypes.number]).isRequired,
    index: PropTypes.number.isRequired,
    itemProps: PropTypes.object.isRequired,
    selectedItem: PropTypes.string.isRequired,
    suggestion: PropTypes.shape({
        label: PropTypes.string.isRequired,
    }).isRequired,
};

function DownshiftMultiple(props) {
    const {classes} = props;
    const [inputValue, setInputValue] = React.useState('');
    const [selectedItem, setSelectedItem] = React.useState([]);
    const [titles, setTitlesValue] = React.useState([]);
    const [commonPersons, setCommonPersonsValue] = React.useState('');
    const client = new ApolloClient({uri: '/graphql',});

    function getTitlesQuery(searchString) {
        return gql`{films(search: "%${searchString}%", limit: 20) {id, title}}`
    }

    function getCommonNamesQuery() {
        return gql`{commonPersons(titles: ${JSON.stringify(selectedItem)}) {name}}`
    }

    const setTitlesDebounced = throttle(300, debounce(300, setTitles));

    function setTitles(searchString) {
        if (searchString.length < 3) {
            setTitlesValue([]);
            return
        }
        client.query(
            {query: getTitlesQuery(searchString)}
            ).then(result => {
            let films = result.data.films.map(film => {
                return {
                    title: film.title,
                }
            });
            setTitlesValue(films)
        })
    }

    function setCommonPersons() {
        client.query(
            {query: getCommonNamesQuery()}
            ).then(result => {
                if (!result.data.commonPersons.length) {
                    setCommonPersonsValue(<label>No common persons</label>);
                    return
                }
                setCommonPersonsValue(<div>
                    <ul>
                        {result.data.commonPersons.map(item => <li key={item.name}>{item.name}</li>)}
                    </ul>
                </div>)
        })
    }

    function handleKeyDown(event) {
        if (selectedItem.length && !inputValue.length && event.key === 'Enter') {
            setCommonPersons();
            return
        }
        if (selectedItem.length && !inputValue.length && event.key === 'Backspace') {
            setSelectedItem(selectedItem.slice(0, selectedItem.length - 1));
            setCommonPersonsValue('')
        }
    }

    function handleInputChange(event) {
        setInputValue(event.target.value);
        setTitlesDebounced(event.target.value);
        setCommonPersonsValue('')
    }

    function handleChange(item) {
        let newSelectedItem = [...selectedItem];
        if (newSelectedItem.indexOf(item) === -1) {
            newSelectedItem = [...newSelectedItem, item];
        }
        setInputValue('');
        setSelectedItem(newSelectedItem);
    }

    const handleDelete = item => () => {
        const newSelectedItem = [...selectedItem];
        newSelectedItem.splice(newSelectedItem.indexOf(item), 1);
        setSelectedItem(newSelectedItem);
        setCommonPersonsValue('')
    };

    return (
        <div>
            <Downshift
                id="downshift-multiple"
                inputValue={inputValue}
                onChange={handleChange}
                selectedItem={selectedItem}
            >
                {({
                      getInputProps,
                      getItemProps,
                      getLabelProps,
                      isOpen,
                      selectedItem: selectedItem,
                      highlightedIndex,
                  }) => {
                    const {onBlur, onChange, onFocus, ...inputProps} = getInputProps({
                        onKeyDown: handleKeyDown,
                        placeholder: 'Select multiple films',
                    });

                    return (
                        <div className={classes.container}>
                            {renderInput({
                                fullWidth: true,
                                classes,
                                label: 'Films',
                                InputLabelProps: getLabelProps(),
                                InputProps: {
                                    startAdornment: selectedItem.map(item => (
                                        <Chip
                                            key={item}
                                            tabIndex={-1}
                                            label={item}
                                            className={classes.chip}
                                            onDelete={handleDelete(item)}
                                        />
                                    )),
                                    onBlur,
                                    onChange: event => {
                                        handleInputChange(event);
                                        onChange(event);
                                    },
                                    onFocus,
                                },
                                inputProps,
                            })}

                            {isOpen ? (
                                <Paper className={classes.paper} square>
                                    {titles.map((suggestion, index) =>
                                        renderSuggestion({
                                            suggestion,
                                            index,
                                            itemProps: getItemProps({item: suggestion.title}),
                                            highlightedIndex,
                                            selectedItem: selectedItem,
                                        }),
                                    )}
                                </Paper>
                            ) : null}
                        </div>
                    );
                }}
            </Downshift>
            {commonPersons}
        </div>
    );
}

DownshiftMultiple.propTypes = {
    classes: PropTypes.object.isRequired,
};

const useStyles = makeStyles(theme => ({
    root: {
        flexGrow: 1,
        height: 250,
    },
    container: {
        flexGrow: 1,
        position: 'relative',
    },
    paper: {
        position: 'absolute',
        zIndex: 1,
        marginTop: theme.spacing(1),
        left: 0,
        right: 0,
    },
    chip: {
        margin: theme.spacing(0.5, 0.25),
    },
    inputRoot: {
        flexWrap: 'wrap',
    },
    inputInput: {
        width: 'auto',
        flexGrow: 1,
    },
    divider: {
        height: theme.spacing(2),
    },
}));

export default function FilmsUi() {
    const classes = useStyles();
    return (
        <div className={classes.root}>
            <DownshiftMultiple classes={classes}/>
        </div>
    );
}